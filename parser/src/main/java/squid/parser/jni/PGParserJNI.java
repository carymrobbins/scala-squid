package squid.parser.jni;

public class PGParserJNI {
  public static class PGParseResult {
    public final String parseTree;
    public final PGParseError error;

    public PGParseResult(String parseTree, PGParseError error) {
      this.parseTree = parseTree;
      this.error = error;
    }
  }

  public static class PGParseError {
    public final String message;
    public final int line;
    public final int cursorPos;

    public PGParseError(String message, int line, int cursorPos) {
      this.message = message;
      this.line = line;
      this.cursorPos = cursorPos;
    }
  }

  public static native PGParseResult nativeParse(String sql);

  private static native void nativeInit();

  static {
    System.loadLibrary("PGParserJNI");
    nativeInit();
  }
}
