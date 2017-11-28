/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License"). You may not use this file except in
 * compliance with the License. A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package exercise.util;

import java.math.BigInteger;
import java.util.Random;

public class Utils {
  private static final Random RANDOM = new Random();

  /**
   * @return A random unsigned 128-bit int converted to a decimal string.
   */
  public static String randomExplicitHashKey() {
    return new BigInteger(128, RANDOM).toString(10);
  }
}
