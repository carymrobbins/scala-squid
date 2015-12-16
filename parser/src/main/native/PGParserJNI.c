#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <jni.h>
#include <pg_query.h>
#include "squid_parser_jni_PGParserJNI.h"

JNIEXPORT void JNICALL Java_squid_parser_jni_PGParserJNI_nativeInit
  (JNIEnv *env, jclass cls)
{
  pg_query_init();
}

JNIEXPORT jstring JNICALL Java_squid_parser_jni_PGParserJNI_nativeParse
  (JNIEnv * env, jclass cls, jstring sql)
{
  char* msg;
  const char* sql_buf = (*env)->GetStringUTFChars(env, sql, 0);
  PgQueryParseResult parsed = pg_query_parse(sql_buf);
  (*env)->ReleaseStringUTFChars(env, sql, sql_buf);
  jstring ret = 0;
  if (parsed.error) {
    size_t err_msg_size;
    err_msg_size = snprintf(NULL, 0,
      "error: %s at %d", parsed.error->message, parsed.error->cursorpos
    );
    msg = malloc(err_msg_size + 1);
    if (msg == NULL) {
      msg = "error: could not allocate memory for error message";
    } else {
      snprintf(msg, err_msg_size,
        "error: %s at %d", parsed.error->message, parsed.error->cursorpos
      );
    }
  } else {
    msg = parsed.parse_tree;
  }
  ret = (*env)->NewStringUTF(env, msg);
  pg_query_free_parse_result(parsed);
  return ret;
}
