# Overlay port: identical to upstream vcpkg's krb5 port (1.22.2, port-version 0)
# except that it builds from the official MIT release tarball instead of the
# GitHub tag archive. The release tarball ships a pre-generated `configure`, so
# we drop AUTOCONFIG and never run autoreconf — the manylinux images used by
# duckdb/extension-ci-tools carry autoconf 2.73, whose autoreconf rejects
# krb5's K5_AC_INIT wrapper ("AC_INIT not found").
# Windows support is dropped: this extension only ships krb5 on linux/osx.
vcpkg_download_distfile(ARCHIVE
    URLS "https://kerberos.org/dist/krb5/1.22/krb5-${VERSION}.tar.gz"
         "https://web.mit.edu/kerberos/dist/krb5/1.22/krb5-${VERSION}.tar.gz"
    FILENAME "krb5-${VERSION}.tar.gz"
    SHA512 3237cacfb2019285107991a3211e0d74944c605942ab38a8b4b372703b8f02f5779fa2de80c4e201bd59703d557f37ac346bdc5ea14b986b0a0db23eb422fc6f
)

vcpkg_extract_source_archive(SOURCE_PATH
    ARCHIVE "${ARCHIVE}"
    PATCHES
        static-deps.diff
        define-des-zeroblock.diff
)

vcpkg_configure_make(
    SOURCE_PATH "${SOURCE_PATH}/src"
    OPTIONS
        --disable-nls
        --with-tls-impl=no
        "CFLAGS=-fcommon \$CFLAGS"
)
vcpkg_install_make()

vcpkg_replace_string("${CURRENT_PACKAGES_DIR}/tools/${PORT}/bin/krb5-config" "${CURRENT_INSTALLED_DIR}" [[$(cd "$(dirname "$0")/../../.."; pwd -P)]])
vcpkg_replace_string("${CURRENT_PACKAGES_DIR}/tools/${PORT}/bin/compile_et" "${CURRENT_INSTALLED_DIR}" [[$(cd "$(dirname "$0")/../../.."; pwd -P)]])
if(NOT VCPKG_BUILD_TYPE)
    vcpkg_replace_string("${CURRENT_PACKAGES_DIR}/tools/${PORT}/debug/bin/krb5-config" "${CURRENT_INSTALLED_DIR}" [[$(cd "$(dirname "$0")/../../../.."; pwd -P)]])
    vcpkg_replace_string("${CURRENT_PACKAGES_DIR}/tools/${PORT}/debug/bin/compile_et" "${CURRENT_INSTALLED_DIR}" [[$(cd "$(dirname "$0")/../../../.."; pwd -P)]])
endif()

vcpkg_fixup_pkgconfig()

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/share")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/var")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/share/krb5/cat1")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/share/krb5/cat5")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/share/krb5/cat7")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/share/krb5/cat8")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/var")

if(VCPKG_LIBRARY_LINKAGE STREQUAL "static")
    file(REMOVE_RECURSE
        "${CURRENT_PACKAGES_DIR}/debug/lib/krb5/"
        "${CURRENT_PACKAGES_DIR}/lib/krb5/"
    )
endif()

if(VCPKG_BUILD_TYPE)
  file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug")
endif()

vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/NOTICE")
