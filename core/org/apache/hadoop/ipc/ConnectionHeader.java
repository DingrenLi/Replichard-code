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
package org.apache.hadoop.ipc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * The IPC connection header sent by the client to the server
 * on connection establishment.
 */
class ConnectionHeader implements Writable {
  public static final Log LOG = LogFactory.getLog(ConnectionHeader.class);
  
  private String protocol;
  private UserGroupInformation ugi = new UnixUserGroupInformation();
  public String maprJobName;
  
  public ConnectionHeader() {}
  
  /**
   * Create a new {@link ConnectionHeader} with the given <code>protocol</code>
   * and {@link UserGroupInformation}. 
   * @param protocol protocol used for communication between the IPC client
   *                 and the server
   * @param ugi {@link UserGroupInformation} of the client communicating with
   *            the server
   */
  public ConnectionHeader(String protocol, UserGroupInformation ugi) {
    this.protocol = protocol;
    this.ugi = ugi;
  }
  public void setJobName(String maprJobName){
    this.maprJobName=maprJobName;
  }
  @Override
  public void readFields(DataInput in) throws IOException {
    String protocol1 = Text.readString(in);
    int colonIndex = protocol1.indexOf(':');

    if (colonIndex!=-1) {
      protocol = protocol1.substring(0, colonIndex);
      maprJobName = protocol1.substring(colonIndex + 1);
      if (protocol.isEmpty()) {
        protocol = null;
      }
      if (maprJobName.isEmpty()){
        maprJobName="first";
      }
    }


    boolean ugiPresent = in.readBoolean();
    if (ugiPresent) {
      ugi.readFields(in);
    } else {
      ugi = null;
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {

    protocol=protocol+":"+maprJobName;
    LOG.info("protocal is"+protocol);
    Text.writeString(out, (protocol == null) ? "" : protocol);
    if (ugi != null) {
      out.writeBoolean(true);
      ugi.write(out);
    } else {
      out.writeBoolean(false);
    }
  }

  public String getProtocol() {
    return protocol;
  }

  public UserGroupInformation getUgi() {
    return ugi;
  }

  public String toString() {
    return protocol + "-" + ugi;
  }

  public String getMaprJobName(){
    if(maprJobName.isEmpty())
      maprJobName="first";
    return maprJobName;}
}
