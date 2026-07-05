//===----------------------------------------------------------------------===//
//                         DuckDB
//
// hms_kerberos.hpp
//
// Kerberos/GSSAPI authentication for the Hive Metastore Thrift connection.
// Wraps a TSocket in a Thrift SASL transport that authenticates with the GSSAPI
// (Kerberos v5) mechanism, driving libgssapi_krb5 directly so the ambient
// credential cache (kinit / KRB5CCNAME) and krb5.conf are used with no DuckDB
// secrets required. GSSAPI is a hard dependency and is always compiled in.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

#include <memory>
#include <thrift/transport/TTransport.h>

namespace duckdb {

// Wrap an (unopened) underlying Thrift transport — typically a TSocket — in a
// Thrift SASL/GSSAPI client transport. On open() it performs the Kerberos
// handshake against service principal "<service>/<fqdn>@REALM" using the
// ambient credential cache, and thereafter length-frames every message exactly
// like Hive's own TSaslTransport (QOP=auth, no wrapping).
std::shared_ptr<apache::thrift::transport::TTransport>
HMSMakeKerberosTransport(std::shared_ptr<apache::thrift::transport::TTransport> underlying, const string &service,
                         const string &fqdn);

} // namespace duckdb
