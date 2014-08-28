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
# EUDATePIDremoveForce(*path)
# checkMeta(*source,*AName,*AValue)
# getMeta( *source, *AName , *AValue )
# countMetaKeys( *source , *AName , *AValue )
# ingestObject( *source )
# transferInitiated( *source )
# transferFinished( *source )
# changeValueinICAT(*source, *key , *newval )

#
# This function remove an ePID... even if its 10320/loc field is not empty!
# To be improved.       
#
# Arguments:
#   *path           [IN]    The path of the object to be removed
#
# Author: S Coutin (CINES) based on code from Giacomo Mariani, CINECA - 
#
EUDATePIDremoveForce(*path) {
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
# Check if ROR and email address have been inserted in ICAT by the user
#
# Parameters:
#       *source  [IN]    target object
#       *AName   [IN]    name of the MD that has been modified
#       *AValue  [IN]    value of the MD that has been modified
#
# Author: Pascal Dug�nie, CINES
# updated : St�phane Coutin (CINES) - 26/8/14 (Use ROR instead of EUDAT_ROR as attribute name)
#-----------------------------------------------------------------------------
checkMeta(*source,*AName,*AValue)
{
	if ((*AName == "n:ADMIN_Status") && ( *AValue == "v:ReadyToArchive"))
	{
		ingestObject(*source);
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
# Author: Pascal Dug�nie, CINES
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
# Author: Pascal Dug�nie, CINES
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
# Manage the ingestion in B2SAFE
#	Check the checksum
#	Create PID
#
# Parameters:
#       *source  [IN]    target object to assign a PID
#
# Author: Stephane Coutin (CINES) 
#-----------------------------------------------------------------------------

ingestObject( *source )
{

	logInfo("ingestObject-> Check for (*source)");
	msiDataObjChksum(*source, "null", *checksum);

	getMeta(       *source , "OTHER_original_checksum"  , *orig_checksum   );

	if ( *checksum == *orig_checksum )
	{
		logInfo("ingestObject-> Checksum is same as original = *checksum");
        	changeValueinICAT(*source, "ADMIN_Status" , "Checksum_ok" ) ;

		# Extract the ROR value from iCat
		getMeta( *source, "EUDAT/ROR" , *RorValue )	

 		EUDATCreatePID("None", *source, *RorValue, bool("true"), *PID);
		# test PID creation
		if((*PID == "empty") || (*PID == "None")) {
			logInfo("ingestObject-> ERROR while creating the PID for *source PID = *PID");
			changeValueinICAT(*source, "ADMIN_Status" , "ErrorPID" ) ;
		}
		else {
			logInfo("ingestObject-> PID created for *source PID = [*PID] ROR = [*RorValue]");
	        	changeValueinICAT(*source, "ADMIN_Status" , "Archive_ok" ) ;
		}
	}
	else
	{
		logInfo("ingestObject-> Checksum (*checksum) is different than original (*orig_checksum)");
		changeValueinICAT(*source, "ADMIN_Status" , "ErrorChecksum" ) ;
	}
        changeValueinICAT(*source, "INFO_Checksum" , *checksum ); 
}



#-----------------------------------------------------------------------------
# Process executed when a transfer has been initiated
# (this process is triggered by the iputPreProc hook)
#
# Parameters:
#       *source  [IN]    target object to assign a new value
#
# Author: Pascal Dug�nie, CINES
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
# Author: Pascal Dug�nie, CINES
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
# Author: Pascal Dug�nie, CINES
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


