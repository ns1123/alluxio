package alluxio.underfs.multi;

public class InputOutput<T,U> {
  private T mInput;
  private U mOutput;

  public InputOutput(T input, U output) {
    mInput = input;
    mOutput = output;
  }

  public T getInput() {
    return mInput;
  }

  public U getOutput() {
    return mOutput;
  }
}
