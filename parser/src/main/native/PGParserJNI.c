#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <jni.h>
#include <pg_query.h>
#include "squid_parser_jni_PGParserJNI.h"

/* Helper constants for finding classes and building method signatures. */
#define CLASS_PGParserJNI "squid/parser/jni/PGParserJNI"
#define CLASS_PGParseError CLASS_PGParserJNI "$PGParseError"
#define CLASS_PGParseResult CLASS_PGParserJNI "$PGParseResult"
#define CLASS_String "java/lang/String"
#define PARAM(cls) "L" cls ";"

JNIEXPORT void JNICALL Java_squid_parser_jni_PGParserJNI_nativeInit
  (JNIEnv *env, jclass cls)
{
  pg_query_init();
}

JNIEXPORT jobject JNICALL Java_squid_parser_jni_PGParserJNI_nativeParse
  (JNIEnv * env, jclass cls, jstring sql)
{
  /* Required values to construct a java PGParseResult. */
  jstring parseTree = NULL;
  jobject errorObj = NULL;
  /* Get a char* ref from our java String and give it to pg_query. */
  const char* sql_buf = (*env)->GetStringUTFChars(env, sql, 0);
  PgQueryParseResult result = pg_query_parse(sql_buf);
  (*env)->ReleaseStringUTFChars(env, sql, sql_buf);
  if (result.error) {
    /* If we have an error, construct a java PGParseError. */
    jclass errorClass = (*env)->FindClass(env, CLASS_PGParseError);
    jmethodID errorCtor = (*env)->GetMethodID(env, errorClass, "<init>",
      "(" PARAM(CLASS_String) "II)V"
    );
    errorObj = (*env)->NewObject(env, errorClass, errorCtor,
      (*env)->NewStringUTF(env, result.error->message),
      result.error->lineno,
      result.error->cursorpos
    );
  } else {
    /* Otherwise, we get a java String from the parse tree. */
    parseTree = (*env)->NewStringUTF(env, result.parse_tree);
  }
  /* We don't need the original parse result anymore. */
  pg_query_free_parse_result(result);
  /* Construct the java PGParseResult to be returned. */
  jclass resultClass = (*env)->FindClass(env, CLASS_PGParseResult);
  jmethodID resultCtor = (*env)->GetMethodID(env, resultClass, "<init>",
    "(" PARAM(CLASS_String) PARAM(CLASS_PGParseError) ")V"
  );
  return (*env)->NewObject(env, resultClass, resultCtor, parseTree, errorObj);
}
