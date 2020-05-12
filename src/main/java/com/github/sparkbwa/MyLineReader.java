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

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

/**
 * A class that provides a line reader from an input stream. Depending on the
 * constructor used, lines will either be terminated by:
 * <ul>
 * <li>one of the following: '\n' (LF) , '\r' (CR), or '\r\n' (CR+LF).</li>
 * <li><em>or</em>, a custom byte sequence delimiter</li>
 * </ul>
 * In both cases, EOF also terminates an otherwise unterminated line.
 */
@InterfaceAudience.LimitedPrivate({ "MapReduce" })
@InterfaceStability.Unstable
public class MyLineReader implements Closeable {
    private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
    private int bufferSize = DEFAULT_BUFFER_SIZE;
    private InputStream in;
    private byte[] buffer;
    // the number of bytes of real data in the buffer
    private int bufferLength = 0;
    // the current position in the buffer
    private int bufferPosn = 0;

    private static final byte CR = '\r';
    private static final byte LF = '\n';

    // The line delimiter
    private final byte[] recordDelimiterBytes;

    /**
     * Create a line reader that reads from the given stream using the default
     * buffer-size (64k).
     * 
     * @param in
     *            The input stream
     * @throws IOException
     */
    public MyLineReader(InputStream in) {
        this(in, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Create a line reader that reads from the given stream using the given
     * buffer-size.
     * 
     * @param in
     *            The input stream
     * @param bufferSize
     *            Size of the read buffer
     * @throws IOException
     */
    public MyLineReader(InputStream in, int bufferSize) {
        this.in = in;
        this.bufferSize = bufferSize;
        this.buffer = new byte[this.bufferSize];
        this.recordDelimiterBytes = null;
    }

    /**
     * Create a line reader that reads from the given stream using the
     * <code>io.file.buffer.size</code> specified in the given
     * <code>Configuration</code>.
     * 
     * @param in
     *            input stream
     * @param conf
     *            configuration
     * @throws IOException
     */
    public MyLineReader(InputStream in, Configuration conf) throws IOException {
        this(in, conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE));
    }

    /**
     * Create a line reader that reads from the given stream using the default
     * buffer-size, and using a custom delimiter of array of bytes.
     * 
     * @param in
     *            The input stream
     * @param recordDelimiterBytes
     *            The delimiter
     */
    public MyLineReader(InputStream in, byte[] recordDelimiterBytes) {
        this.in = in;
        this.bufferSize = DEFAULT_BUFFER_SIZE;
        this.buffer = new byte[this.bufferSize];
        this.recordDelimiterBytes = recordDelimiterBytes;
    }

    /**
     * Create a line reader that reads from the given stream using the given
     * buffer-size, and using a custom delimiter of array of bytes.
     * 
     * @param in
     *            The input stream
     * @param bufferSize
     *            Size of the read buffer
     * @param recordDelimiterBytes
     *            The delimiter
     * @throws IOException
     */
    public MyLineReader(InputStream in, int bufferSize, byte[] recordDelimiterBytes) {
        this.in = in;
        this.bufferSize = bufferSize;
        this.buffer = new byte[this.bufferSize];
        this.recordDelimiterBytes = recordDelimiterBytes;
    }

    /**
     * Create a line reader that reads from the given stream using the
     * <code>io.file.buffer.size</code> specified in the given
     * <code>Configuration</code>, and using a custom delimiter of array of
     * bytes.
     * 
     * @param in
     *            input stream
     * @param conf
     *            configuration
     * @param recordDelimiterBytes
     *            The delimiter
     * @throws IOException
     */
    public MyLineReader(InputStream in, Configuration conf, byte[] recordDelimiterBytes) throws IOException {
        this.in = in;
        this.bufferSize = conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE);
        this.buffer = new byte[this.bufferSize];
        this.recordDelimiterBytes = recordDelimiterBytes;
    }

    /**
     * Close the underlying stream.
     * 
     * @throws IOException
     */
    public void close() throws IOException {
        in.close();
    }

    /**
     * Read one line from the InputStream into the given Text.
     *
     * @param str
     *            the object to store the given line (without newline)
     * @param maxLineLength
     *            the maximum number of bytes to store into str; the rest of the
     *            line is silently discarded.
     * @param maxBytesToConsume
     *            the maximum number of bytes to consume in this call. This is
     *            only a hint, because if the line cross this threshold, we
     *            allow it to happen. It can overshoot potentially by as much as
     *            one buffer length.
     *
     * @return the number of bytes read including the (longest) newline found.
     *
     * @throws IOException
     *             if the underlying stream throws
     */
    public int readLine(Text str, int maxLineLength, int maxBytesToConsume) throws IOException {
        if (this.recordDelimiterBytes != null) {
            return readCustomLine(str, maxLineLength, maxBytesToConsume);
        } else {
            return readDefaultLine(str, maxLineLength, maxBytesToConsume);
        }
    }

    public int readLine(Text str, int maxLineLength, int maxBytesToConsume, boolean clear) throws IOException {
        if (this.recordDelimiterBytes != null) {
            return readCustomLine(str, maxLineLength, maxBytesToConsume,clear);
        } else {
            return readDefaultLine(str, maxLineLength, maxBytesToConsume,clear);
        }
    }

    protected int fillBuffer(InputStream in, byte[] buffer, boolean inDelimiter) throws IOException {
        return in.read(buffer);
    }

    private int readDefaultLine(Text str, int maxLineLength, int maxBytesToConsume, boolean clear) throws IOException {
        if (clear) {
            str.clear();
        }
        int txtLength = 0; // tracks str.getLength(), as an optimization
        int newlineLength = 0; // length of terminating newline
        boolean prevCharCR = false; // true of prev char was CR
        long bytesConsumed = 0;
        do {
            int startPosn = bufferPosn; // starting from where we left off the
                                        // last time
            if (bufferPosn >= bufferLength) {
                startPosn = bufferPosn = 0;
                if (prevCharCR) {
                    ++bytesConsumed; // account for CR from previous read
                }
                bufferLength = fillBuffer(in, buffer, prevCharCR);
                if (bufferLength <= 0) {
                    break; // EOF
                }
            }
            for (; bufferPosn < bufferLength; ++bufferPosn) { // search for
                                                                // newline
                if (buffer[bufferPosn] == LF) {
                    newlineLength = (prevCharCR) ? 2 : 1;
                    ++bufferPosn; // at next invocation proceed from following
                                    // byte
                    break;
                }
                if (prevCharCR) { // CR + notLF, we are at notLF
                    newlineLength = 1;
                    break;
                }
                prevCharCR = (buffer[bufferPosn] == CR);
            }
            int readLength = bufferPosn - startPosn;
            if (prevCharCR && newlineLength == 0) {
                --readLength; // CR at the end of the buffer
            }
            bytesConsumed += readLength;
            int appendLength = readLength - newlineLength;
            if (appendLength > maxLineLength - txtLength) {
                appendLength = maxLineLength - txtLength;
            }
            if (appendLength > 0) {
                str.append(buffer, startPosn, appendLength);
                txtLength += appendLength;
            }
        } while (newlineLength == 0 && bytesConsumed < maxBytesToConsume);

        if (bytesConsumed > Integer.MAX_VALUE) {
            throw new IOException("Too many bytes before newline: " + bytesConsumed);
        }
        return (int) bytesConsumed;
    }

    /**
     * Read a line terminated by one of CR, LF, or CRLF.
     */
    private int readDefaultLine(Text str, int maxLineLength, int maxBytesToConsume) throws IOException {
        str.clear();
        int txtLength = 0; // tracks str.getLength(), as an optimization
        int newlineLength = 0; // length of terminating newline
        boolean prevCharCR = false; // true of prev char was CR
        long bytesConsumed = 0;
        do {
            int startPosn = bufferPosn; // starting from where we left off the
                                        // last time
            if (bufferPosn >= bufferLength) {
                startPosn = bufferPosn = 0;
                if (prevCharCR) {
                    ++bytesConsumed; // account for CR from previous read
                }
                bufferLength = fillBuffer(in, buffer, prevCharCR);
                if (bufferLength <= 0) {
                    break; // EOF
                }
            }
            for (; bufferPosn < bufferLength; ++bufferPosn) { // search for
                                                                // newline
                if (buffer[bufferPosn] == LF) {
                    newlineLength = (prevCharCR) ? 2 : 1;
                    ++bufferPosn; // at next invocation proceed from following
                                    // byte
                    break;
                }
                if (prevCharCR) { // CR + notLF, we are at notLF
                    newlineLength = 1;
                    break;
                }
                prevCharCR = (buffer[bufferPosn] == CR);
            }
            int readLength = bufferPosn - startPosn;
            if (prevCharCR && newlineLength == 0) {
                --readLength; // CR at the end of the buffer
            }
            bytesConsumed += readLength;
            int appendLength = readLength - newlineLength;
            if (appendLength > maxLineLength - txtLength) {
                appendLength = maxLineLength - txtLength;
            }
            if (appendLength > 0) {
                str.append(buffer, startPosn, appendLength);
                txtLength += appendLength;
            }
        } while (newlineLength == 0 && bytesConsumed < maxBytesToConsume);

        if (bytesConsumed > Integer.MAX_VALUE) {
            throw new IOException("Too many bytes before newline: " + bytesConsumed);
        }
        return (int) bytesConsumed;
    }

    private int readCustomLine(Text str, int maxLineLength, int maxBytesToConsume, boolean clear) throws IOException {
        if (clear) {
            str.clear();
        }
        int txtLength = 0; // tracks str.getLength(), as an optimization
        long bytesConsumed = 0;
        int delPosn = 0;
        int ambiguousByteCount = 0; // To capture the ambiguous characters count
        do {
            int startPosn = bufferPosn; // Start from previous end position
            if (bufferPosn >= bufferLength) {
                startPosn = bufferPosn = 0;
                bufferLength = fillBuffer(in, buffer, ambiguousByteCount > 0);
                if (bufferLength <= 0) {
                    if (ambiguousByteCount > 0) {
                        str.append(recordDelimiterBytes, 0, ambiguousByteCount);
                        bytesConsumed += ambiguousByteCount;
                    }
                    break; // EOF
                }
            }
            for (; bufferPosn < bufferLength; ++bufferPosn) {
                if (buffer[bufferPosn] == recordDelimiterBytes[delPosn]) {
                    delPosn++;
                    if (delPosn >= recordDelimiterBytes.length) {
                        bufferPosn++;
                        break;
                    }
                } else if (delPosn != 0) {
                    bufferPosn--;
                    delPosn = 0;
                }
            }
            int readLength = bufferPosn - startPosn;
            bytesConsumed += readLength;
            int appendLength = readLength - delPosn;
            if (appendLength > maxLineLength - txtLength) {
                appendLength = maxLineLength - txtLength;
            }
            bytesConsumed += ambiguousByteCount;
            if (appendLength >= 0 && ambiguousByteCount > 0) {
                // appending the ambiguous characters (refer case 2.2)
                str.append(recordDelimiterBytes, 0, ambiguousByteCount);
                ambiguousByteCount = 0;
                // since it is now certain that the split did not split a
                // delimiter we
                // should not read the next record: clear the flag otherwise
                // duplicate
                // records could be generated
                unsetNeedAdditionalRecordAfterSplit();
            }
            if (appendLength > 0) {
                str.append(buffer, startPosn, appendLength);
                txtLength += appendLength;
            }
            if (bufferPosn >= bufferLength) {
                if (delPosn > 0 && delPosn < recordDelimiterBytes.length) {
                    ambiguousByteCount = delPosn;
                    bytesConsumed -= ambiguousByteCount; // to be consumed in
                                                            // next
                }
            }
        } while (delPosn < recordDelimiterBytes.length && bytesConsumed < maxBytesToConsume);
        if (bytesConsumed > Integer.MAX_VALUE) {
            throw new IOException("Too many bytes before delimiter: " + bytesConsumed);
        }
        return (int) bytesConsumed;
    }

    /**
     * Read a line terminated by a custom delimiter.
     */
    private int readCustomLine(Text str, int maxLineLength, int maxBytesToConsume) throws IOException {
        str.clear();
        int txtLength = 0; // tracks str.getLength(), as an optimization
        long bytesConsumed = 0;
        int delPosn = 0;
        int ambiguousByteCount = 0; // To capture the ambiguous characters count
        do {
            int startPosn = bufferPosn; // Start from previous end position
            if (bufferPosn >= bufferLength) {
                startPosn = bufferPosn = 0;
                bufferLength = fillBuffer(in, buffer, ambiguousByteCount > 0);
                if (bufferLength <= 0) {
                    if (ambiguousByteCount > 0) {
                        str.append(recordDelimiterBytes, 0, ambiguousByteCount);
                        bytesConsumed += ambiguousByteCount;
                    }
                    break; // EOF
                }
            }
            for (; bufferPosn < bufferLength; ++bufferPosn) {
                if (buffer[bufferPosn] == recordDelimiterBytes[delPosn]) {
                    delPosn++;
                    if (delPosn >= recordDelimiterBytes.length) {
                        bufferPosn++;
                        break;
                    }
                } else if (delPosn != 0) {
                    bufferPosn--;
                    delPosn = 0;
                }
            }
            int readLength = bufferPosn - startPosn;
            bytesConsumed += readLength;
            int appendLength = readLength - delPosn;
            if (appendLength > maxLineLength - txtLength) {
                appendLength = maxLineLength - txtLength;
            }
            bytesConsumed += ambiguousByteCount;
            if (appendLength >= 0 && ambiguousByteCount > 0) {
                // appending the ambiguous characters (refer case 2.2)
                str.append(recordDelimiterBytes, 0, ambiguousByteCount);
                ambiguousByteCount = 0;
                // since it is now certain that the split did not split a
                // delimiter we
                // should not read the next record: clear the flag otherwise
                // duplicate
                // records could be generated
                unsetNeedAdditionalRecordAfterSplit();
            }
            if (appendLength > 0) {
                str.append(buffer, startPosn, appendLength);
                txtLength += appendLength;
            }
            if (bufferPosn >= bufferLength) {
                if (delPosn > 0 && delPosn < recordDelimiterBytes.length) {
                    ambiguousByteCount = delPosn;
                    bytesConsumed -= ambiguousByteCount; // to be consumed in
                                                            // next
                }
            }
        } while (delPosn < recordDelimiterBytes.length && bytesConsumed < maxBytesToConsume);
        if (bytesConsumed > Integer.MAX_VALUE) {
            throw new IOException("Too many bytes before delimiter: " + bytesConsumed);
        }
        return (int) bytesConsumed;
    }

    /**
     * Read from the InputStream into the given Text.
     * 
     * @param str
     *            the object to store the given line
     * @param maxLineLength
     *            the maximum number of bytes to store into str.
     * @return the number of bytes read including the newline
     * @throws IOException
     *             if the underlying stream throws
     */
    public int readLine(Text str, int maxLineLength) throws IOException {
        return readLine(str, maxLineLength, Integer.MAX_VALUE);
    }

    /**
     * Read from the InputStream into the given Text.
     * 
     * @param str
     *            the object to store the given line
     * @return the number of bytes read including the newline
     * @throws IOException
     *             if the underlying stream throws
     */
    public int readLine(Text str) throws IOException {
        return readLine(str, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    protected int getBufferPosn() {
        return bufferPosn;
    }

    protected int getBufferSize() {
        return bufferSize;
    }

    protected void unsetNeedAdditionalRecordAfterSplit() {
        // needed for custom multi byte line delimiters only
        // see MAPREDUCE-6549 for details
    }
}