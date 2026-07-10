include_directories(include)

add_subdirectory(thrift)

add_subdirectory(storage)

add_library(
  hms_ext_library OBJECT
  hive_metastore_extension.cpp
  hms_utils.cpp
  hms_format_detector.cpp
  hms_path_utils.cpp
  hms_client.cpp
  hms_api.cpp
  hms_config.cpp
  hms_kerberos.cpp
  hms_connection.cpp)
# Link to hms_thrift_lib to get Thrift includes and library
target_link_libraries(hms_ext_library PRIVATE hms_thrift_lib)

# --- Kerberos/GSSAPI (required) -------------------------------------------
# Authenticating to a secured Hive Metastore uses SASL/GSSAPI. GSSAPI is a hard
# dependency and is always compiled in. It is provided by the vcpkg `krb5` port
# (see vcpkg.json), so the library is vendored into the extension binary and no
# machine-specific paths or ambient krb5 install are required. Discovery is via
# the krb5-gssapi.pc that vcpkg installs (its pkgconfig dir is placed on
# PKG_CONFIG_PATH by the vcpkg toolchain); a plain find_* is the fallback.
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
  pkg_check_modules(GSSAPI IMPORTED_TARGET GLOBAL krb5-gssapi)
endif()
if(TARGET PkgConfig::GSSAPI)
  target_link_libraries(hms_ext_library PRIVATE PkgConfig::GSSAPI)
  set(HMS_KERBEROS_LIBS
      PkgConfig::GSSAPI
      PARENT_SCOPE)
  message(STATUS "HMS: GSSAPI via pkg-config krb5-gssapi (${GSSAPI_VERSION})")
else()
  # Fallback: the vcpkg toolchain puts its install prefix on CMAKE_PREFIX_PATH,
  # so these resolve to the vendored MIT krb5 (gssapi_krb5 + its static chain).
  find_path(GSSAPI_INCLUDE_DIR NAMES gssapi/gssapi.h REQUIRED)
  find_library(GSSAPI_KRB5_LIBRARY NAMES gssapi_krb5 REQUIRED)
  find_library(KRB5_LIBRARY NAMES krb5)
  find_library(K5CRYPTO_LIBRARY NAMES k5crypto)
  find_library(COM_ERR_LIBRARY NAMES com_err)
  target_include_directories(hms_ext_library PRIVATE ${GSSAPI_INCLUDE_DIR})
  set(_hms_krb_libs ${GSSAPI_KRB5_LIBRARY})
  foreach(_lib ${KRB5_LIBRARY} ${K5CRYPTO_LIBRARY} ${COM_ERR_LIBRARY})
    if(_lib)
      list(APPEND _hms_krb_libs ${_lib})
    endif()
  endforeach()
  set(HMS_KERBEROS_LIBS
      ${_hms_krb_libs}
      PARENT_SCOPE)
  message(STATUS "HMS: GSSAPI via ${GSSAPI_KRB5_LIBRARY}")
endif()

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:hms_ext_library>
    $<TARGET_OBJECTS:hms_thrift_lib>
    PARENT_SCOPE)
