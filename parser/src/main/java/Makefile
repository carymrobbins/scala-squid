all:
	javac ../java/squid/parser/jni/PGParserJNI.java
	javah -jni squid.parser.jni.PGParserJNI
	gcc -shared -fpic -o libPGParserJNI.so -I/usr/lib/jvm/java-8-oracle/include -I/usr/lib/jvm/java-8-oracle/include/linux -I$(HOME)/build/libpg_query -L$(HOME)/build/libpg_query PGParserJNI.c $(HOME)/build/libpg_query/libpg_query.a

clean:
	rm -f PGParserJNI.so PGParserJNI.h PGParserJNI.class
