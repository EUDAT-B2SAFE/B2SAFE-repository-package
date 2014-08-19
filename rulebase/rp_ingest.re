################################################################################
#                                                                              #
# EUDAT Safe-Replication and PID management policies                           #
#                                                                              #
################################################################################

################################################################################
#                                                                              #
# Ingestion functions for repository packages                                  #
#                                                                              #
################################################################################

# List of the functions:
#  (tbc)


########################## SCN reprise du code B2STAGE de Giacomo(could be removed)

EUDATiDSSfileWrite(*DSSfile) {
          writeLine ("serverLog","Test PID -> acPostProcForCopy checks for *DSSfile");
          msiSplitPath(*DSSfile, *coll, *name);
          *b = bool("false");
          *d = SELECT count(DATA_NAME) WHERE COLL_NAME like '*coll' AND DATA_NAME = '*name';
          foreach(*c in *d) {
              msiGetValByKey(*c,"DATA_NAME",*num);
              if(*num == '1') {
                  *b = bool("true");
              }
          }
          if (!*b)
          {  
              writeLine ("serverLog","Test PID -> acPostProcForCopy creates *DSSfile");
              *OFlagsB="destRescName=CinecaData++++forceFlag=";
              msiDataObjCreate(*DSSfile,*OFlagsB,*DSSf);
              msiDataObjClose(*DSSf,*Status);
          }
          writeLine ("serverLog","Test PID -> acPostProcForCopy *DSSfile exists");
          msiDataObjOpen(*DSSfile++"++++openFlags=O_RDWR",*DSSf);
          writeLine ("serverLog","Test PID -> acPostProcForCopy Open OK.");
          msiDataObjLseek(*DSSf,"null","SEEK_END",*Status);
          writeLine ("serverLog","Test PID -> acPostProcForCopy Seek OK.");
#          EUDATiPIDretrieve($objPath, *PID)
	  EUDATiFieldVALUEretrieve($objPath, "PID", *PID)
	  writeLine("stdout","PID => *PID");
          getEpicApiParameters(*credStoreType, *credStorePath, *epicApi, *serverID, *epicDebug)
          *Buf="*serverID"++"$objPath,*PID\n";
          msiDataObjWrite(*DSSf,*Buf,*Len);
          writeLine ("serverLog","Test PID -> acPostProcForCopy Write OK.");
          msiDataObjClose(*DSSf,*Status);
          writeLine ("serverLog","Test PID -> acPostProcForCopy Close OK.");
}
     

EUDATgetObjectAge(*filePath, *age) {
          # Look when the file has been created in iRODS
          msiSplitPath(*filePath, *fileDir, *fileName);
          *ec = SELECT DATA_CREATE_TIME, DATA_MODIFY_TIME WHERE DATA_NAME = '*fileName' AND COLL_NAME = '*fileDir';
          foreach(*ec) {
              msiGetValByKey(*ec, "DATA_CREATE_TIME", *creationTime);
              msiWriteRodsLog("EUDATgetObjectAge -> Created at *creationTime", *status);
              msiGetValByKey(*ec, "DATA_MODIFY_TIME", *modifyTime);
              msiWriteRodsLog("EUDATgetObjectAge -> Modified at *modifyTime", *status);
          }
          msiGetSystemTime(*Now,"unix");
          msiWriteRodsLog("EUDATgetObjectAge -> Actual time *Now", *status);
          *age=int(*Now)-int(*modifyTime);
          msiWriteRodsLog("EUDATgetObjectAge -> Time from last access: *age seconds", *status);
}

########################## SCN fin du code B2STAGE de Giacomo


########## specific CUNI
###
###


checkMeta_test(*source,*AName,*AValue)
{


logInfo( "PDU in checkmeta");


}



#-----------------------------------------------------------------------------
# Check if ROR and email address have been inserted in ICAT by the user
#
# Parameters:
#       *source  [IN]    target object
#       *AName   [IN]    name of the MD that has been modified
#       *AValue  [IN]    value of the MD that has been modified
#
# Author: Pascal Dugénie, CINES
#-----------------------------------------------------------------------------
checkMeta(*source,*AName,*AValue)
{

	assign ( *email_optional , "yes" );

	assign ( *res0           , "0"   );

	countMetaKeys( *source , "EUDAT_ROR"      , *nb_res1	);

	countMetaKeys( *source , "OTHER_AckEmail" , *nb_res2	);

        countMetaKeys( *source , "OTHER_original_checksum" , *nb_res3   );

	getMeta(       *source , "ADMIN_Status"  , *res0   );

	logInfo("Check metadata in iCAT for (*source): *res0 , *nb_res1 , *nb_res2 , *nb_res3");


	if ( *res0 == "TransferFinished" )
	{

		if ( *email_optional == "yes" )
		{

	    		if( (*nb_res1 > "0") && (*nb_res3 > "0") )
			{

				checkChecksum(*source);
#				insertPIDinICAT(*source);
			}

		}
		else
		{
	    	  if( (*nb_res1 != "0") && (*nb_res2 != "0") && ( (*AName == "EUDAT_ROR" ) ||  (*AName == "OTHER_AckEmail") ) )
		  {

			checkChecksum( *source );
#			insertPIDinICAT(*source);
		  }
		}
	}



#        if( (*nb_res1 != "0") && (*nb_res3 != "0") )
#        {
#
#                insertPIDinICAT(*source);
#        }



}


#-----------------------------------------------------------------------------
# get values of metadata in ICAT
#
# Parameters:
#       *source  [IN]    target object
#       *AName   [IN]    name of the MD
#       *AValue  [OUT]   value of the MD
#
# Author: Pascal Dugénie, CINES
#-----------------------------------------------------------------------------
getMeta( *source, *AName , *AValue )
{

   	msiSplitPath(*source,*parent,*child);

	msiExecStrCondQuery( "SELECT META_DATA_ATTR_VALUE WHERE META_DATA_ATTR_NAME = '*AName' AND COLL_NAME = '*parent' AND DATA_NAME = '*child'" , *B );

        foreach   ( *B )    {
               	msiGetValByKey( *B , "META_DATA_ATTR_VALUE" , *AValue );
        }

}


#-----------------------------------------------------------------------------
# count metadata in ICAT
#
# Parameters:
#       *source  [IN]    target object
#       *AName   [IN]    name of the MD
#       *AValue  [OUT]   value of the MD
#
# Author: Pascal Dugénie, CINES
#-----------------------------------------------------------------------------
countMetaKeys( *source , *AName , *AValue )
{

	msiSplitPath(*source, *parent , *child );

	msiExecStrCondQuery( "SELECT count(META_DATA_ATTR_VALUE) WHERE META_DATA_ATTR_NAME = '*AName' AND COLL_NAME = '*parent' AND DATA_NAME = '*child'" , *B );

	foreach   ( *B )    {
		msiGetValByKey( *B , "META_DATA_ATTR_VALUE" , *AValue );
		logInfo("########## Avalue= *AValue");
	}

}


#-----------------------------------------------------------------------------
# Compare checksum for every new object
#
# Parameters:
#       *source  [IN]    target object to assign a PID
#
# Author: Pascal Dugénie, CINES
#-----------------------------------------------------------------------------

checkChecksum( *source )
{

	msiDataObjChksum(*source, "null", *checksum);

	getMeta(       *source , "OTHER_original_checksum"  , *orig_checksum   );

	if ( *checksum == *orig_checksum )
	{

		insertPIDinICAT(*source , *checksum);	
	}

	else
	{
		changeValueinICAT(*source, "ADMIN_Status" , "ErrorChecksum" ) ;
	}

       changeValueinICAT(*source, "INFO_Checksum" , *checksum );

}






#-----------------------------------------------------------------------------
# Insert a PID record in ICAT for every new object
#
# Parameters:
#       *source  [IN]    target object to assign a PID
#
# Author: Pascal Dugénie, CINES
#-----------------------------------------------------------------------------

insertPIDinICAT(*source , *checksum) {

    logInfo("Insert PID in iCAT for (*source)");

    getSharedCollection(*source,*collectionPath);
    logInfo( "Get collection path: *collectionPath" );

    getEpicApiParameters(*credStoreType, *credStorePath, *epicApi, *serverID, *epicDebug);
    logInfo( "Credential info: *credStoreType, *credStorePath, *epicApi, *serverID, *epicDebug" );

#	msiDataObjChksum(*source, "null", *checksum);


    msiExecCmd("epicclient.py","*credStoreType *credStorePath create *serverID*source --checksum *checksum","null","null", "null", *out);

#    msiExecCmd("epicclient.py","os /opt/iRODS/modules/EUDAT-PID/cmd/credentials_example test","null","null", "null", *out);

#msiExecCmd("rndString",5,"null","null","null",*out);

    msiGetStdoutInExecCmdOut(*out, *PIDresponse);
    logInfo("PID response: *PIDresponse");

    msiGetObjType( *source , *objType);

   msiExecCmd("epicclient.py","*credStoreType *credStorePath modify *PIDresponse ROR *PIDresponse", "null", "null", "null", *out2);



#    msiGetSystemTime(*TimeNow,"human");

	# returns the size of a string
#	msiStrlen(*source, *stringlen );


#	assign( *ror , *stringlen );

#	msiSubstr(*source, "15" , "8" , *ror );

#	logInfo( "Calculate RoR *ror from object *source" );


#    msiAddKeyVal( *TimeKey , "INFO_TimeOfTransfer"   , *TimeNow       );
   msiAddKeyVal( *PIDkey  , "EUDAT_PID"           	, *PIDresponse   );


#    msiAddKeyVal( *Chkkey  , "INFO_Checksum"       	, *checksum      );

	changeValueinICAT(*source, "ADMIN_Status" , "Archive_ok" ) ;


#    msiAssociateKeyValuePairsToObj(*TimeKey , *source , *objType );
    msiAssociateKeyValuePairsToObj(*PIDkey  , *source , *objType );
#    msiAssociateKeyValuePairsToObj(*Chkkey  , *source , *objType );

}


#-----------------------------------------------------------------------------
# Process executed when a transfer has been initiated
# (this process is triggered by the iputPreProc hook)
#
# Parameters:
#       *source  [IN]    target object to assign a new value
#
# Author: Pascal Dugénie, CINES
#-----------------------------------------------------------------------------

transferInitiated( *source )
{

        changeValueinICAT(*source, "ADMIN_Status" , "TransferStarted" ) ;

	msiGetSystemTime( *TimeNow, "human" );

        changeValueinICAT(*source, "INFO_TimeOfStart", *TimeNow ) ;

}



#-----------------------------------------------------------------------------
# Process executed after a transfer is finished
# (this process is triggered by the iputPostProc hook)
#
# Parameters:
#       *source  [IN]    target object to assign a new value
#
# Author: Pascal Dugénie, CINES
#-----------------------------------------------------------------------------

transferFinished( *source )
{

	changeValueinICAT(*source, "ADMIN_Status" , "TransferFinished" ) ;

	msiGetSystemTime( *TimeNow, "human" );

	changeValueinICAT(*source, "INFO_TimeOfTransfer" , *TimeNow ) ;

}




#-----------------------------------------------------------------------------
# Change a value in iCAT
#
# Parameters:
#       *source  [IN]    target object to assign a new value
#       *key     [IN]    target key    to assign a new value
#       *newval  [IN]    new value to be assigned
#
# Author: Pascal Dugénie, CINES
#-----------------------------------------------------------------------------

changeValueinICAT(*source, *key , *newval )
{

	msiSplitPath(  *source , *parent 	, *child		);

    msiGetObjType( *source , *objType					);

	countMetaKeys( *source , *key 		, *key_exist 	);


	logInfo( "Set *key into *newval (key_exist=*key_exist)" );


	if ( *key_exist != "0" ){

		msiExecStrCondQuery( "SELECT META_DATA_ATTR_VALUE WHERE META_DATA_ATTR_NAME = '*key' AND COLL_NAME = '*parent' AND DATA_NAME = '*child'" , *B );

		foreach   ( *B )    {
			msiGetValByKey( *B , "META_DATA_ATTR_VALUE" , *val )	;
		}

		msiAddKeyVal(                   *mdkey  , *key    , *val      );
		msiRemoveKeyValuePairsFromObj(  *mdkey  , *source , *objType  );
	}



	msiAddKeyVal(                   *mdkey  , *key    , *newval   );

	msiAssociateKeyValuePairsToObj( *mdkey  , *source , *objType  );

}



#-----------------------------------------------------------------------------
# Insert a Status record in ICAT for every new object
#
# Parameters:
#       *source  [IN]    target object
#
# Author: Pascal Dugénie, CINES
#-----------------------------------------------------------------------------

# This function is NOT USED anymore

insertStatusinICAT(*source)
{

    logInfo("Modify ADMIN_Status for (*source)")									;

    msiGetSystemTime( *TimeNow , "human" )											;


	changeValueinICAT( *source, "INFO_TimeOfDataUpload" , *TimeNow               )	;

	changeValueinICAT( *source, "ADMIN_Status"          , "Waiting for metadata" )	;



}



#-----------------------------------------------------------------------------
#	msiSendMail( 'dugenie@cines.fr' , "subject" , "body" );
#	msiExecCmd("mail.sh","null","null","null", "null", *out);

#
# once the data has been controlled, move it to another resource
#  msiDataObjPhymv






