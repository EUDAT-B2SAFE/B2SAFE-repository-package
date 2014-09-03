################################################################################
#                                                                              #
# EUDAT Safe-Replication and PID management policies                           #
#                                                                              #
################################################################################

################################################################################
#                                                                              #
# Ingestion functions for repository packages                                  #
# Author : S Coutin (CINES), P Dugenie (CINES)                                 #
#                                                                              #
# Requires EUDAT core v2.1                                                     #
#                                                                              #
# Status : Ongoing development - Last update 25/8/2014                         #
#                                                                              #
################################################################################

# List of the functions:
# ----------------------
# rp_EUDATePIDremoveForce(*path)
# rp_checkMeta(*source,*AName,*AValue)
# rp_getMeta( *source, *AName , *AValue )
# rp_countMetaKeys( *source , *AName , *AValue )
# rp_ingestObject( *source )
# rp_transferInitiated( *source )
# rp_transferFinished( *source )
# rp_changeValueinICAT(*source, *key , *newval )

#
# This function remove an ePID... even if its 10320/loc field is not empty!
# To be improved.       
#
# Arguments:
#   *path           [IN]    The path of the object to be removed
#
# Author: S Coutin (CINES) based on code from Giacomo Mariani, CINECA - 
#
rp_EUDATePIDremoveForce(*path) {
    getEpicApiParameters(*credStoreType, *credStorePath, *epicApi, *serverID, *epicDebug) 
    logInfo("EUDATePIDremove -> Removing PID associated to: $userNameClient, $objPath ($filePath)");

    if (EUDATSearchPID(*path, *pid)) {
        msiExecCmd("epicclient.py","*credStoreType *credStorePath read --key 10320/LOC *pid", "null", "null", "null", *out2);
        msiGetStdoutInExecCmdOut(*out2, *loc10320);
        logInfo("EUDATePIDremove -> get 10320/LOC from handle response = *loc10320");
        
        msiExecCmd("epicclient.py","*credStoreType *credStorePath delete *pid",
                     "null", "null", "null", *out3);
        msiGetStdoutInExecCmdOut(*out3, *response3);
        logInfo("EUDATePIDremove -> delete handle response = *response3");
        # The PID record could be associated to a replica.
        # The field 10320/LOC of the parent PID record should be updated
    }
    else {
        logInfo("EUDATePIDremove -> No PID associated to *path found");
    }
} 


#-----------------------------------------------------------------------------
# Check if the ADMIN_Status value is set to Ready ToArchive and then kicks off ingestion
#
# Parameters:
#       *source  [IN]    target object
#       *AName   [IN]    name of the MD that has been modified
#       *AValue  [IN]    value of the MD that has been modified
#
# Author: Pascal Dugénie, CINES
# updated : Stéphane Coutin (CINES) - 26/8/14 (Use ROR instead of EUDAT_ROR as attribute name)
#-----------------------------------------------------------------------------
rp_checkMeta(*source,*AName,*AValue)
{
	if ((*AName == "n:ADMIN_Status") && ( *AValue == "v:ReadyToArchive"))
	{
		rp_ingestObject(*source);
	}
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
rp_getMeta( *source, *AName , *AValue )
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
rp_countMetaKeys( *source , *AName , *AValue )
{
	msiSplitPath(*source, *parent , *child );
	msiExecStrCondQuery( "SELECT count(META_DATA_ATTR_VALUE) WHERE META_DATA_ATTR_NAME = '*AName' AND COLL_NAME = '*parent' AND DATA_NAME = '*child'" , *B );

	foreach   ( *B )    {
		msiGetValByKey( *B , "META_DATA_ATTR_VALUE" , *AValue );
		logInfo("########## Avalue= *AValue");
	}
}


#-----------------------------------------------------------------------------
# Manage the ingestion in B2SAFE
#	Check the checksum
#	Create PID
#
# Parameters:
#       *source  [IN]    target object to assign a PID
#
# Author: Stephane Coutin (CINES) 
#-----------------------------------------------------------------------------

rp_ingestObject( *source )
{

	logInfo("ingestObject-> Check for (*source)");
	msiDataObjChksum(*source, "null", *checksum);

	rp_getMeta(       *source , "OTHER_original_checksum"  , *orig_checksum   );

	if ( *checksum == *orig_checksum )
	{
		logInfo("ingestObject-> Checksum is same as original = *checksum");
        	rp_changeValueinICAT(*source, "ADMIN_Status" , "Checksum_ok" ) ;

		# Extract the ROR value from iCat
		rp_getMeta( *source, "EUDAT/ROR" , *RorValue )	

 		EUDATCreatePID("None", *source, *RorValue, bool("true"), *PID);
		# test PID creation
		if((*PID == "empty") || (*PID == "None")) {
			logInfo("ingestObject-> ERROR while creating the PID for *source PID = *PID");
			rp_changeValueinICAT(*source, "ADMIN_Status" , "ErrorPID" ) ;
		}
		else {
			logInfo("ingestObject-> PID created for *source PID = [*PID] ROR = [*RorValue]");
	        	rp_changeValueinICAT(*source, "ADMIN_Status" , "Archive_ok" ) ;
		}
	}
	else
	{
		logInfo("ingestObject-> Checksum (*checksum) is different than original (*orig_checksum)");
		rp_changeValueinICAT(*source, "ADMIN_Status" , "ErrorChecksum" ) ;
	}
        rp_changeValueinICAT(*source, "INFO_Checksum" , *checksum ); 
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

rp_transferInitiated( *source )
{

        rp_changeValueinICAT(*source, "ADMIN_Status" , "TransferStarted" ) ;

	msiGetSystemTime( *TimeNow, "human" );

        rp_changeValueinICAT(*source, "INFO_TimeOfStart", *TimeNow ) ;

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

rp_transferFinished( *source )
{

	rp_changeValueinICAT(*source, "ADMIN_Status" , "TransferFinished" ) ;
	msiGetSystemTime( *TimeNow, "human" );
	rp_changeValueinICAT(*source, "INFO_TimeOfTransfer" , *TimeNow ) ;
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

rp_changeValueinICAT(*source, *key , *newval )
{
	msiSplitPath(  *source , *parent 	, *child		);
	msiGetObjType( *source , *objType					);
	rp_countMetaKeys( *source , *key 		, *key_exist 	);
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



###############################################################################################################################
########## Below this line, the functions are left for backward compatibility
###############################################################################################################################
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





