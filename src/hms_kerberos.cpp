#include "hms_kerberos.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <gssapi/gssapi.h>
#include <gssapi/gssapi_krb5.h>
#include <thrift/transport/TTransportException.h>
#include <thrift/transport/TVirtualTransport.h>

namespace duckdb {

using apache::thrift::transport::TTransport;
using apache::thrift::transport::TTransportException;

namespace {

// Thrift SASL negotiation status bytes (see TSaslTransport / NegotiationStatus).
enum SaslStatus : uint8_t {
	SASL_START = 0x01,
	SASL_OK = 0x02,
	SASL_BAD = 0x03,
	SASL_ERROR = 0x04,
	SASL_COMPLETE = 0x05,
};

// Guard against absurd length prefixes from a misbehaving/hostile peer.
constexpr uint32_t MAX_SASL_MESSAGE = 100u * 1024u * 1024u;

// Bit for "no security layer" in the SASL GSSAPI QOP bitmask (RFC 4752).
constexpr unsigned char LAYER_NONE = 0x01;

// Shown when the peer doesn't behave like a SASL endpoint — almost always a
// configuration mismatch between hive-site.xml and the actual metastore.
constexpr const char *kNonSaslServerHint =
    "The Hive Metastore did not speak the SASL/Kerberos protocol. This usually means "
    "hive.metastore.sasl.enabled=true in hive-site.xml but the metastore itself is not kerberized (or vice versa). "
    "Verify the metastore's authentication configuration.";

void PutBE32(uint8_t *out, uint32_t v) {
	out[0] = static_cast<uint8_t>((v >> 24) & 0xFF);
	out[1] = static_cast<uint8_t>((v >> 16) & 0xFF);
	out[2] = static_cast<uint8_t>((v >> 8) & 0xFF);
	out[3] = static_cast<uint8_t>(v & 0xFF);
}

uint32_t GetBE32(const uint8_t *in) {
	return (static_cast<uint32_t>(in[0]) << 24) | (static_cast<uint32_t>(in[1]) << 16) |
	       (static_cast<uint32_t>(in[2]) << 8) | static_cast<uint32_t>(in[3]);
}

// Render a GSSAPI major/minor status pair into a human-readable message.
string GssErrorString(const string &prefix, OM_uint32 maj_stat, OM_uint32 min_stat) {
	string msg = prefix;
	for (int which = 0; which < 2; which++) {
		OM_uint32 code = (which == 0) ? maj_stat : min_stat;
		int type = (which == 0) ? GSS_C_GSS_CODE : GSS_C_MECH_CODE;
		OM_uint32 msg_ctx = 0;
		do {
			gss_buffer_desc status_string = GSS_C_EMPTY_BUFFER;
			OM_uint32 min2 = 0;
			OM_uint32 m = gss_display_status(&min2, code, type, GSS_C_NO_OID, &msg_ctx, &status_string);
			if (status_string.value) {
				msg += ": ";
				msg.append(static_cast<const char *>(status_string.value), status_string.length);
				gss_release_buffer(&min2, &status_string);
			}
			if (GSS_ERROR(m)) {
				break;
			}
		} while (msg_ctx != 0);
	}
	return msg;
}

// Drives the client side of the SASL "GSSAPI" (Kerberos v5) mechanism directly
// against libgssapi_krb5. Mirrors Cyrus SASL's gssapi_client_mech_step: first a
// GSS security-context negotiation, then a one-round security-layer (QOP)
// exchange in which we advertise/select "no layer" (QOP=auth).
class GssClient {
public:
	GssClient(string service, string fqdn) : service_(std::move(service)), fqdn_(std::move(fqdn)) {
	}

	~GssClient() {
		OM_uint32 min_stat = 0;
		if (ctx_ != GSS_C_NO_CONTEXT) {
			gss_delete_sec_context(&min_stat, &ctx_, GSS_C_NO_BUFFER);
		}
		if (target_ != GSS_C_NO_NAME) {
			gss_release_name(&min_stat, &target_);
		}
	}

	// GSSAPI always produces an initial client token.
	bool HasInitialResponse() const {
		return true;
	}

	bool IsComplete() const {
		return complete_;
	}

	// Feed a server challenge (empty for the very first call), returning the
	// next client token to send.
	string Step(const string &challenge) {
		switch (state_) {
		case State::AUTHNEG:
			return StepAuth(challenge);
		case State::SSFCAP:
			return StepSecurityLayer(challenge);
		default:
			throw IOException("GSSAPI step called after handshake completion");
		}
	}

private:
	enum class State { AUTHNEG, SSFCAP, DONE };

	void ImportTargetName() {
		// Build the SPN the way Hive's Java clients (Hive CLI, Spark, beeline)
		// effectively do: "service/instance", where the instance — the
		// principal's concrete instance, or the connect host for _HOST — is
		// used as given, just lowercased (and any resolver trailing dot
		// stripped). Import it as a krb5 principal name so krb5 never rewrites
		// it via DNS: MIT's host-based canonicalization would (e.g. append the
		// resolver's search domain), which the JVM stack does not do.
		string instance = StringUtil::Lower(fqdn_);
		if (!instance.empty() && instance.back() == '.') {
			instance.pop_back();
		}
		string sname = service_ + "/" + instance;
		gss_buffer_desc name_buf;
		name_buf.length = sname.size();
		name_buf.value = const_cast<char *>(sname.data());
		OM_uint32 min_stat = 0;
		OM_uint32 maj_stat = gss_import_name(&min_stat, &name_buf, GSS_KRB5_NT_PRINCIPAL_NAME, &target_);
		if (GSS_ERROR(maj_stat)) {
			throw IOException(GssErrorString("GSSAPI gss_import_name failed for '" + sname + "'", maj_stat, min_stat));
		}
	}

	string StepAuth(const string &challenge) {
		if (target_ == GSS_C_NO_NAME) {
			ImportTargetName();
		}

		gss_buffer_desc in_tok;
		in_tok.length = challenge.size();
		in_tok.value = const_cast<char *>(challenge.data());
		gss_buffer_t in_ptr = challenge.empty() ? GSS_C_NO_BUFFER : &in_tok;

		gss_buffer_desc out_tok = GSS_C_EMPTY_BUFFER;
		// RFC 4752 requires INTEG; MUTUAL gives us server authentication. We do
		// not request CONF — we only ever select QOP=auth (no security layer).
		OM_uint32 req_flags = GSS_C_MUTUAL_FLAG | GSS_C_SEQUENCE_FLAG | GSS_C_INTEG_FLAG;
		OM_uint32 ret_flags = 0;
		OM_uint32 min_stat = 0;
		OM_uint32 maj_stat =
		    gss_init_sec_context(&min_stat, GSS_C_NO_CREDENTIAL, &ctx_, target_, gss_mech_krb5, req_flags, 0,
		                         GSS_C_NO_CHANNEL_BINDINGS, in_ptr, nullptr, &out_tok, &ret_flags, nullptr);

		string response;
		if (out_tok.length > 0) {
			response.assign(static_cast<const char *>(out_tok.value), out_tok.length);
			OM_uint32 min2 = 0;
			gss_release_buffer(&min2, &out_tok);
		}

		if (GSS_ERROR(maj_stat)) {
			throw IOException(GssErrorString("GSSAPI gss_init_sec_context failed (is there a valid Kerberos ticket? "
			                                 "run kinit)",
			                                 maj_stat, min_stat));
		}
		if (maj_stat == GSS_S_COMPLETE) {
			// Security context established; move on to the QOP exchange. Note we
			// are NOT yet SASL-complete: the server still sends its layer offer.
			state_ = State::SSFCAP;
		}
		return response;
	}

	string StepSecurityLayer(const string &challenge) {
		gss_buffer_desc in_tok;
		in_tok.length = challenge.size();
		in_tok.value = const_cast<char *>(challenge.data());
		gss_buffer_desc out_tok = GSS_C_EMPTY_BUFFER;
		OM_uint32 min_stat = 0;
		OM_uint32 maj_stat = gss_unwrap(&min_stat, ctx_, &in_tok, &out_tok, nullptr, nullptr);
		if (GSS_ERROR(maj_stat)) {
			throw IOException(GssErrorString("GSSAPI gss_unwrap of security-layer token failed", maj_stat, min_stat));
		}
		if (out_tok.length != 4) {
			OM_uint32 min2 = 0;
			gss_release_buffer(&min2, &out_tok);
			throw IOException("GSSAPI security-layer token had unexpected length (expected 4)");
		}
		unsigned char server_layers = static_cast<const unsigned char *>(out_tok.value)[0];
		OM_uint32 min3 = 0;
		gss_release_buffer(&min3, &out_tok);

		if ((server_layers & LAYER_NONE) == 0) {
			throw IOException("The Hive Metastore requires a SASL security layer (integrity/privacy QOP). This "
			                  "extension currently supports only QOP=auth. Set hive.metastore.thrift.sasl.qop=auth on "
			                  "the metastore, or add wrapped-QOP support to the extension.");
		}

		// Advertise "no security layer" and a zero max-buffer (no wrapping).
		unsigned char resp[4] = {LAYER_NONE, 0, 0, 0};
		gss_buffer_desc resp_in;
		resp_in.length = sizeof(resp);
		resp_in.value = resp;
		gss_buffer_desc resp_out = GSS_C_EMPTY_BUFFER;
		// conf_req_flag = 0 → integrity-only protection of this token itself.
		maj_stat = gss_wrap(&min_stat, ctx_, 0, GSS_C_QOP_DEFAULT, &resp_in, nullptr, &resp_out);
		if (GSS_ERROR(maj_stat)) {
			throw IOException(GssErrorString("GSSAPI gss_wrap of security-layer response failed", maj_stat, min_stat));
		}
		string response(static_cast<const char *>(resp_out.value), resp_out.length);
		OM_uint32 min4 = 0;
		gss_release_buffer(&min4, &resp_out);

		complete_ = true;
		state_ = State::DONE;
		return response;
	}

	string service_;
	string fqdn_;
	State state_ = State::AUTHNEG;
	bool complete_ = false;
	gss_ctx_id_t ctx_ = GSS_C_NO_CONTEXT;
	gss_name_t target_ = GSS_C_NO_NAME;
};

// Thrift transport that performs the SASL/GSSAPI handshake on open() and then
// length-frames every message, byte-for-byte compatible with Hive's Java
// TSaslTransport (with QOP=auth, so no per-frame wrap/unwrap).
class HMSSaslGssapiTransport : public apache::thrift::transport::TVirtualTransport<HMSSaslGssapiTransport> {
public:
	HMSSaslGssapiTransport(std::shared_ptr<TTransport> underlying, const string &service, const string &fqdn)
	    : underlying_(std::move(underlying)), gss_(service, fqdn) {
	}

	bool isOpen() const override {
		return opened_ && underlying_->isOpen();
	}

	bool peek() override {
		return read_pos_ < read_buf_.size() || underlying_->peek();
	}

	void open() override {
		if (opened_) {
			return;
		}
		if (!underlying_->isOpen()) {
			underlying_->open();
		}
		Handshake();
		opened_ = true;
	}

	void close() override {
		underlying_->close();
		opened_ = false;
	}

	uint32_t read(uint8_t *buf, uint32_t len) {
		if (read_pos_ >= read_buf_.size()) {
			ReadFrame();
		}
		uint32_t avail = static_cast<uint32_t>(read_buf_.size() - read_pos_);
		uint32_t n = std::min(len, avail);
		if (n > 0) {
			std::memcpy(buf, read_buf_.data() + read_pos_, n);
			read_pos_ += n;
		}
		return n;
	}

	void write(const uint8_t *buf, uint32_t len) {
		write_buf_.append(reinterpret_cast<const char *>(buf), len);
	}

	void flush() override {
		uint32_t data_length = static_cast<uint32_t>(write_buf_.size());
		uint8_t len_prefix[4];
		PutBE32(len_prefix, data_length);
		underlying_->write(len_prefix, 4);
		if (data_length > 0) {
			underlying_->write(reinterpret_cast<const uint8_t *>(write_buf_.data()), data_length);
		}
		underlying_->flush();
		write_buf_.clear();
	}

private:
	void SendSaslMessage(uint8_t status, const string &payload) {
		uint8_t header[5];
		header[0] = status;
		PutBE32(header + 1, static_cast<uint32_t>(payload.size()));
		underlying_->write(header, 5);
		if (!payload.empty()) {
			underlying_->write(reinterpret_cast<const uint8_t *>(payload.data()),
			                   static_cast<uint32_t>(payload.size()));
		}
		underlying_->flush();
	}

	// Reads one framed SASL negotiation message. Throws on BAD/ERROR, surfacing
	// the server's diagnostic string (e.g. "GSS initiate failed").
	uint8_t ReceiveSaslMessage(string &payload) {
		uint8_t header[5];
		underlying_->readAll(header, 5);
		uint8_t status = header[0];
		uint32_t len = GetBE32(header + 1);
		// A non-kerberized (plaintext) metastore replying to our negotiation
		// frame won't produce a valid SASL status byte; the same for a garbled
		// length. Surface the likely misconfiguration instead of a cryptic
		// parse error or an oversized allocation.
		if (status < SASL_START || status > SASL_COMPLETE) {
			throw TTransportException(kNonSaslServerHint);
		}
		if (len > MAX_SASL_MESSAGE) {
			throw TTransportException("SASL negotiation frame from the Hive Metastore is implausibly large (" +
			                          std::to_string(len) + " bytes). " + kNonSaslServerHint);
		}
		payload.resize(len);
		if (len > 0) {
			underlying_->readAll(reinterpret_cast<uint8_t *>(&payload[0]), len);
		}
		if (status == SASL_BAD || status == SASL_ERROR) {
			throw TTransportException("Hive Metastore rejected SASL negotiation: " + payload);
		}
		return status;
	}

	void Handshake() {
		// handleSaslStartMessage: send mechanism name, then the initial token.
		string initial = gss_.HasInitialResponse() ? gss_.Step("") : "";
		SendSaslMessage(SASL_START, "GSSAPI");
		SendSaslMessage(gss_.IsComplete() ? SASL_COMPLETE : SASL_OK, initial);

		string payload;
		uint8_t status = 0;
		bool received_any = false;
		while (!gss_.IsComplete()) {
			status = ReceiveSaslMessage(payload);
			received_any = true;
			if (status != SASL_OK && status != SASL_COMPLETE) {
				throw TTransportException(
				    string("Unexpected SASL status from the Hive Metastore during negotiation. ") + kNonSaslServerHint);
			}
			string challenge = gss_.Step(payload);
			// If the server already signalled COMPLETE, we owe it no further token.
			if (status == SASL_COMPLETE) {
				continue;
			}
			SendSaslMessage(gss_.IsComplete() ? SASL_COMPLETE : SASL_OK, challenge);
		}

		// We finished the mechanism on an OK exchange; the server still owes us a
		// final COMPLETE (it verifies our last token before closing negotiation).
		if (!received_any || status == SASL_OK) {
			status = ReceiveSaslMessage(payload);
			if (status != SASL_COMPLETE) {
				throw TTransportException("Expected SASL COMPLETE from Hive Metastore");
			}
		}
	}

	void ReadFrame() {
		uint8_t len_prefix[4];
		underlying_->readAll(len_prefix, 4);
		uint32_t data_length = GetBE32(len_prefix);
		if (data_length > MAX_SASL_MESSAGE) {
			throw TTransportException("SASL data frame too large");
		}
		read_buf_.resize(data_length);
		if (data_length > 0) {
			underlying_->readAll(reinterpret_cast<uint8_t *>(&read_buf_[0]), data_length);
		}
		read_pos_ = 0;
	}

	std::shared_ptr<TTransport> underlying_;
	GssClient gss_;
	bool opened_ = false;
	string write_buf_;
	string read_buf_;
	size_t read_pos_ = 0;
};

} // namespace

std::shared_ptr<apache::thrift::transport::TTransport>
HMSMakeKerberosTransport(std::shared_ptr<apache::thrift::transport::TTransport> underlying, const string &service,
                         const string &fqdn) {
	return std::make_shared<HMSSaslGssapiTransport>(std::move(underlying), service, fqdn);
}

} // namespace duckdb
