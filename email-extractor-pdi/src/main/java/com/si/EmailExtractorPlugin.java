/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.si;

import emailvalidator4j.EmailValidator;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

import java.util.ArrayList;
import java.util.regex.Matcher;

/**
 * Describe your step plugin.
 * 
 */
public class EmailExtractorPlugin extends BaseStep implements StepInterface {

  private EmailExtractorPluginMeta meta;
  private EmailExtractorPluginData data;

  private static Class<?> PKG = EmailExtractorPluginMeta.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
  
  public EmailExtractorPlugin( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
    Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }
  
  /**
   * Initialize and do work where other steps need to wait for...
   *
   * @param stepMetaInterface
   *          The metadata to work with
   * @param stepDataInterface
   *          The data to initialize
   */
  public boolean init( StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface ) {
    this.data = (EmailExtractorPluginData) stepDataInterface;
    this.meta = (EmailExtractorPluginMeta) stepMetaInterface;
    return super.init( stepMetaInterface, stepDataInterface );
  }

  /**
   * Package rows from emails.
   *
   * @param rmi             The row meta interface.
   * @param emails          The emails
   * @param r               The object row array
   * @return                Double array of rows
   */
  public Object[][] packageRows(RowMetaInterface rmi, ArrayList<String> emails, Object[] r){
    Object[][] orows = new Object[emails.size()][rmi.size()];
    int idx = rmi.indexOfValue(meta.getOutField());
    if(idx >= 0) {
      for (int i = 0; i < emails.size(); i++) {
        String email = emails.get(i);
        Object[] orow = r.clone();
        orow[idx] = email;
        orows[i] = orow;
      }
    }else{
      if(isBasic()){
        logBasic("Output Index for Email extractor does not exist");
      }
    }
    return orows;
  }

  /**
   * Filter valid emails.
   *
   * @param emails      The incoming list of emails
   * @return            An arraylist of valid emails
   */
  private ArrayList<String> filterValidEmails(ArrayList<String> emails){
    ArrayList<String> validEmails = new ArrayList<String>();
    for(String email : emails) {
      EmailValidator validator = new EmailValidator();
      if(validator.isValid(email.trim())){
        validEmails.add(email.trim());
      }
    }
    return validEmails;
  }

  /**
   * Extract emails from the string.
   *
   * @param rmi       The row meta interface
   * @param r         The incoming row
   * @return          An arraylist of potential emails
   */
  private ArrayList<String> extractEmails(RowMetaInterface rmi, Object[] r){
    ArrayList<String> emails = new ArrayList<String>();
    int idx  = rmi.indexOfValue(meta.getInField());
    if(idx >= 0){
      String text = (String) r[idx];
      Matcher m = data.getEmailRegex().matcher(text);
      while(m.find()){
        String potentialEmail = m.group(0);
        emails.add(potentialEmail);
      }
    }else{
      if(isBasic()){
        logBasic("Input field not found for email extractor");
      }
    }
    return emails;
  }

  /**
   * Setup the processor.
   *
   * @throws KettleException
   */
  private void setupProcessor() throws KettleException{
    RowMetaInterface inMeta = getInputRowMeta().clone();
    data.outputRowMeta = inMeta;
    meta.getFields(data.outputRowMeta, getStepname(), null, null, this, null, null);
    first = false;
  }

  /**
   * Send rows to output
   *
   * @param emails              The arraylist of emails
   * @param r                   The object array row representation
   * @throws KettleException
   */
  private void sendRows(ArrayList<String> emails, Object[] r) throws KettleException{
    if(emails.size() > 0){
      if(meta.isCheckValid()){
        emails = filterValidEmails(emails);
      }

      if(emails.size() > 0){
        Object[][] orows = packageRows(data.outputRowMeta, emails, r);
        for(Object[] row : orows){
          putRow(data.outputRowMeta, row);
        }
      }else{
        putRow(data.outputRowMeta, r);
      }
    }else{
      putRow(data.outputRowMeta, r);
    }
  }

  /**
   * Process the row
   *
   * @param smi                     The step meta interface
   * @param sdi                     The step data interface
   * @return                        Whether the row is processed
   * @throws KettleException
   */
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    Object[] r = getRow(); // get row, set busy!
    if ( r == null ) {
      // no more input to be expected...
      setOutputDone();
      return false;
    }

    if(first){
      setupProcessor();
    }

    if(data.outputRowMeta.size() > r.length){
      r = RowDataUtil.resizeArray(r, data.outputRowMeta.size());
    }

    ArrayList<String> emails = extractEmails(data.outputRowMeta, r);
    sendRows(emails, r);

    if ( checkFeedback( getLinesRead() ) ) {
      if ( log.isBasic() )
        logBasic( BaseMessages.getString( PKG, "EmailExtractorPlugin.Log.LineNumber" ) + getLinesRead() );
    }
      
    return true;
  }
}