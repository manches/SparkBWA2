/**
 * Copyright 2016 José Manuel Abuín Mosquera <josemanuel.abuin@usc.es>
 *
 * <p>This file is part of SparkBWA.
 *
 * <p>SparkBWA is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * <p>SparkBWA is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * <p>You should have received a copy of the GNU General Public License along with SparkBWA. If not,
 * see <http://www.gnu.org/licenses/>.
 */
package com.github.sparkbwa;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class MySplitLineReader extends MyLineReader {
  public MySplitLineReader(InputStream in, byte[] recordDelimiterBytes) {
    super(in, recordDelimiterBytes);
  }

  public MySplitLineReader(InputStream in, Configuration conf,
      byte[] recordDelimiterBytes) throws IOException {
    super(in, conf, recordDelimiterBytes);
  }

  public boolean needAdditionalRecordAfterSplit() {
    return false;
  }
}