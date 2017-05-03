package alluxio.underfs.fork;

/**
 * Class for passing both input and output as an argument to {@code Function}.
 *
 * @param <T> the input type
 * @param <U> the output type
 */
class InputOutput<T,U> {
  private T mInput;
  private U mOutput;

  /**
   * Creates a new instance of {@link InputOutput}.
   *
   * @param input the input to use
   * @param output the output to use
   */
  InputOutput(T input, U output) {
    mInput = input;
    mOutput = output;
  }

  /**
   * @return the input
   */
  T getInput() {
    return mInput;
  }

  /**
   * @return the output
   */
  U getOutput() {
    return mOutput;
  }
}
