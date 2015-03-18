/***************************************************************
* MCC Keyword deletion script
* Author Guillaumedrlz
****************************************************************
* Mandatory variables used by this script
* Review these and adapt as needed
***************************************************************/

// The emails who will receive a notification when the script has finished a run
var STATUS_EMAIL_RECEIVER = "YOUR@EMAIL.HERE";
var STATUS_EMAIL_CC = "YOUR@EMAIL.HERE";

// The condition applied to a time range to identify keywords to be deleted
var CONDITION = "Clicks = 0"
var RANGE_START = "20131001" //format YYYYMMDD
var RANGE_END = "20141123" //format YYYYMMDD

// The labels used to mark processed campaigns and accounts
// Limit: 1000 accounts max per label
// If you hit that limit, append a new label to the array below
// e.g. var LABELS_NAME = ["__PROCESSED__","__PROCESSED-1K__"];
// And add the following on line 44
// .withCondition("LabelNames DOES_NOT_CONTAIN '" + LABELS_NAME[1] + "'")
var LABELS_NAME = ["__PROCESSED__"];

/***************************************************************
* This main function identifies the accounts to be processed
* and launches the function accountKeywordDeletion()
* in parallel on the first 50 accounts found
***************************************************************/
function main() {
  // After first run this can throw an error as the labels already exists, but we can ignore.
  for( var n = LABELS_NAME.length-1; n >= 0; n-- ) {
    try {
      MccApp.createAccountLabel( LABELS_NAME[n] );
    } catch (e) {} // Nothing to do about this error
  }      
  
  // Get the list of accounts not yet processed 
  // Maximum 50 due to the parallel execution limit
  var notProcessedAccounts = [];
  var parallelExecutionLimit = 50;
  var accountIterator = MccApp.accounts() 
        .withCondition("LabelNames DOES_NOT_CONTAIN '" + LABELS_NAME[0] + "'")
        .get();
  
  var i = 0;
  while (accountIterator.hasNext() && i < parallelExecutionLimit) {
    var account = accountIterator.next();
    notProcessedAccounts.push( account.getCustomerId() );
    i++;
  }
  
  // If there are no accounts to process, we can clean up
  if( notProcessedAccounts.length == 0) {
    /**
    * This section unlabels all accounts 
    * And deletes the label from the MCC
    * Uncomment this section once all the accounts
    * are labeled and run one last time the script
    **/
    /*
    for( var n = LABELS_NAME.length-1; n >= 0; n-- ) {
      var accountIterator = MccApp.accounts()
        .withCondition("LabelNames CONTAINS '" + LABELS_NAME[n] + "'")
        .get();
    
      while (accountIterator.hasNext()) {
        var account = accountIterator.next();
        account.removeLabel(LABELS_NAME[n]);
      }
    
      // We delete the label from the MCC
      var labelIterator = MccApp.accountLabels()
                              .withCondition("Name ='" + LABELS_NAME[n] + "'")
                              .get();

      while (labelIterator.hasNext()) {
        var label = labelIterator.next();
        label.remove();
      }
    }
    */
    // Send an email to STATUS_EMAIL_RECEIVER with STATUS_EMAIL_CC in cc
    // to indicate the keyword deletion is over
    var body = 'Hello there,'
    +'\n\nAll the accounts have been processed and all the necessary keywords have been deleted.\n\n';
    + '\n\nFor additionnal information, check the logs in the MCC.';
    MailApp.sendEmail({to: STATUS_EMAIL_RECEIVER,
                       cc: STATUS_EMAIL_CC,
                       subject: 'MCC Keyword deletion status report',
                       body: body,
                       noReply: true});
    Logger.log("All the accounts have been processed and all the necessary keywords have been deleted.");

  } else {
    
    // Otherwise, we can process the accounts
    Logger.log('Starting parallel processing on ' + i + ' accounts');
    Logger.log('Accounts in this batch: ' + notProcessedAccounts);
    var accountSelector = MccApp.accounts().withIds(notProcessedAccounts);
    accountSelector.executeInParallel('accountKeywordDeletion', 'batchFinished');
  }
}

/***************************************************************
* This function identifies the campaigns and keywords to
* be processes and deletes the keyword satisfying the criteria
***************************************************************/
function accountKeywordDeletion() {
  // Account completion flag
  var accountDone = 0; 
  
  // After first run this can throw an error as the label already exists, but we can ignore.
  try {
    AdWordsApp.createLabel( LABELS_NAME[0] );
  } catch (e) {} // Nothing to do about this error

  var currentAccountId = AdWordsApp.currentAccount().getCustomerId(); //Current account Id
  var numKwdsDeletedForAccount = 0; // Counter of processed keywords

  /**
  * Select keywords that meet the condition during the date range
  * And that belong to Active or Paused campaigns, Enabled or Paused ad groups
  * (ignoring case)
  **/
  var keywordIterator = AdWordsApp
    .keywords()
    .forDateRange(RANGE_START, RANGE_END)
    .withCondition("CampaignStatus != DELETED")
    .withCondition("AdGroupStatus != DELETED")
    .withCondition(CONDITION)
    .get();

  // Count how many keywords we need to delete
  var numKwds = keywordIterator.totalNumEntities();
  Logger.log("Account: " + currentAccountId + " has "
             + numKwds + " keywords to delete");

  /**
  * We test below if keywords need to be deleted from the account
  * If yes, we grab them campaign per campaign, remove them
  * And label the campaign as done
  **/
  if( numKwds > 0 ) {

    /**
    * Select campaigns that are Active or Paused
    * And that are not labeled as processed
    **/
    var campaignIterator = AdWordsApp
      .campaigns()
      .withCondition("CampaignStatus != DELETED")
      .withCondition("LabelNames CONTAINS_NONE ['" + LABELS_NAME[0] + "']")
      .get();
    
    // We get campaigns to iterate over
    while (campaignIterator.hasNext()) {
      var campaign = campaignIterator.next();
      var numKwdsDeletedForCampaign = 0;
      
      /**
       * Select keywords that meet the condition during the date range
       * And that belong to Enabled or Paused ad groups
       **/
      var campaignKeywordIterator = campaign
        .keywords()
        .forDateRange(RANGE_START, RANGE_END)
        .withCondition(CONDITION)
        .withCondition("QualityScore <= 7")
        .withCondition("AdGroupStatus != DELETED")
        .get();

      // Log how many keywords we need to delete
      var numKwdsCampaign = campaignKeywordIterator.totalNumEntities();
      Logger.log("Account: " + currentAccountId + " has " + numKwdsCampaign
                 + " keywords to delete in campaign '" + campaign.getName() + "'");

      // Start to delete
      while (campaignKeywordIterator.hasNext()) {
        var keyword = campaignKeywordIterator.next();
        keyword.remove();
        // Keep track of the number of deleted KW's
        numKwdsDeletedForAccount++;
        numKwdsDeletedForCampaign++;
      }
      
      if( numKwdsDeletedForCampaign == numKwdsCampaign ) {
        /**
        * We have deleted all eligible keywords from this campaign
        * We now mark it with a label so that we won't
        * Iterate over it the next time
        **/ 
        campaign.applyLabel( LABELS_NAME[0] );   
        Logger.log("Account: " + currentAccountId
                   + " has finished processing campaign '"
                   + campaign.getName());        
        
      } else {
        /**
        * If we hit a processing limit,
        * the campaign may have KW's left
        * we can't label it as processed
        **/         
        Logger.log("Account: " + currentAccountId
                   + ", hasn't finished processing campaign '"+ campaign.getName()
                   + "' (" + numKwdsCampaign-numKwdsDeletedForCampaign + " keywords left)");        
      }
    }
  }

  /**
  * 
  * Select campaigns that are Active or Paused
  * And that are not labeled as processed
  **/
  var campaignCountIterator = AdWordsApp
    .campaigns()
    .withCondition("CampaignStatus != DELETED")
    .withCondition("LabelNames CONTAINS_NONE ['" + LABELS_NAME[0] + "']")
    .get();  
  
  // Count how many campaigns we need to process
  var numCmpgns = campaignCountIterator.totalNumEntities();
    
  // If no keywords need to be deleted or all campaigns are labelled, we clean the labels
  if( numKwds == 0 || numCmpgns == 0 || numKwds == numKwdsDeletedForAccount ) {
    
    campaignIterator = AdWordsApp
       .campaigns()
       .withCondition("LabelNames CONTAINS_ANY['" + LABELS_NAME[0] + "']")
       .get();
    while (campaignIterator.hasNext()) {
      var campaign = campaignIterator.next();
      campaign.removeLabel(LABELS_NAME[0]);
    }
    
    // We delete the label from the account
    var labelIterator = AdWordsApp.labels().withCondition("Name ='" + LABELS_NAME[0] + "'").get();
    while (labelIterator.hasNext()) {
      var label = labelIterator.next();
      label.remove();
    }
    
    // Update the return value
    accountDone = 1;
    Logger.log('Account: ' + currentAccountId + ' is done.');
  }

  // Return values separated by a "/" as a string (mandatory return format)
  return  numKwdsDeletedForAccount.toFixed(0) //number of keywords deleted
  + '/' + (numKwds - numKwdsDeletedForAccount).toFixed(0) //Number of keywords remaining
  + '/' + accountDone.toFixed(0); // Is the account done
}

/***************************************************************
* This function is called when the script has completed a run
* It labels the accounts that are done
* And it sends an email with the return status for all accounts
***************************************************************/
function batchFinished(results) {
  var logs = '';  
    
  // We need to re-create the labels in preview mode due to a bug
  if( AdWordsApp.getExecutionInfo().isPreview() ) {
    logs += 'The script is running in preview mode.\n\n';  
      // After first run this can throw an error as the label already exists, but we can ignore.
      for( var n = LABELS_NAME.length-1; n >= 0; n-- ) {
        try {
          MccApp.createAccountLabel( LABELS_NAME[n] );
        } catch (e) {} // Nothing to do about this error
      }
  }
    
  // Get the ExecutionResult for each account.
  for (var i = 0; i < results.length; i++) {
    var result = results[i];
    var retval = result.getReturnValue();
    logs += 'Customer ID: ' + result.getCustomerId() + '; ';

    // Check the execution status. This can be one of ERROR, TIMEOUT, or OK.
    if ( result.getStatus() == 'ERROR' ) {
      logs += 'Failed with error: ' + result.getError() + '.\n';
    } else if ( result.getStatus() == 'TIMEOUT' || retval === null ) {
      logs += 'Account reached the execution time limit; Additional run(s) are required.\n';
    } else {
      /**
      * If the status is OK then:
      * We split the string returned from the accountKeywordDeletion method
      * to get the return values
      **/
      var retvalNumbers = retval.split('/').map(Number);
      var numKwdsDeleted = retvalNumbers[0];
      var numKwdsRemaining = retvalNumbers[1];
      var accountDone = retvalNumbers[2];
      /**
      * If the account is done
      * We now mark it with a label so that we won't
      * Iterate over it the next time,
      * Otherwise we log the amount processed
      **/
      if( accountDone == 1 ) {
        //No keywords left, we can label as processed
        // Using the latest label
        var account = MccApp.accounts()
          .withIds(["'"+result.getCustomerId()+"'"])
          .get();
        account.next().applyLabel(LABELS_NAME[LABELS_NAME.length-1]);
        logs += 'Account done; Keywords deleted during this run: '
                + numKwdsDeleted + '\n';
        
      } else {
        
        // Keywords are remaining, we log the amount
        logs += 'Processed ' + numKwdsDeleted + ' keywords, '
                + numKwdsRemaining
                + ' left; Additional run(s) are required\n';
      }
    }
  }

  // Some extra information for the status mail
  var intro = 'Hello there,\n\n'
               + 'The following accounts were processsed by the script:\n\n';
  var finish = '\n\nFor additionnal information, check the logs in the MCC.';

  // Send an email to STATUS_EMAIL_RECEIVER and STATUS_EMAIL_CC
  // with the status of the accounts processed during the run.
  MailApp.sendEmail({to: STATUS_EMAIL_RECEIVER,
                     cc: STATUS_EMAIL_CC,
                     subject: 'MCC Keyword deletion status report',
                     body: intro + logs + finish,
                     noReply: true});
  Logger.log(logs);
}
