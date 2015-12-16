package squid.parser.jni;

public class PGParserJNI {
  public static native String nativeParse(String sql);

  private static native void nativeInit();

  static {
    System.loadLibrary("PGParserJNI");
    nativeInit();
  }
}
