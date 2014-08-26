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
# checkChecksum( *source )
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
# Author: Pascal Dugénie, CINES
# updated : Stéphane Coutin (CINES) - 26/8/14 (Use ROR instead of EUDAT_ROR as attribute name)
#-----------------------------------------------------------------------------
checkMeta(*source,*AName,*AValue)
{

	assign ( *email_optional , "yes" );
	assign ( *res0           , "0"   );
	countMetaKeys( *source , "ROR"      , *nb_res1	);
	countMetaKeys( *source , "OTHER_AckEmail" , *nb_res2	);
        countMetaKeys( *source , "OTHER_original_checksum" , *nb_res3   );
	getMeta(       *source , "ADMIN_Status"  , *res0   );

	logInfo("checkMeta -> Check metadata in iCAT for (*source): *res0 , *nb_res1 , *nb_res2 , *nb_res3");

	if ( *res0 == "TransferFinished" )
	{

		if( (*nb_res1 > "0") && (*nb_res3 > "0") )
		{
			checkChecksum(*source);
		}
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
#         Stephane Coutin (CINES) 25/8/2014 - Use EUDAT Core function for PID creation EUDATeiPIDeiChecksumMgmt
#-----------------------------------------------------------------------------

checkChecksum( *source )
{

	logInfo("checkChecksum -> Check for (*source)");
	msiDataObjChksum(*source, "null", *checksum);

	getMeta(       *source , "OTHER_original_checksum"  , *orig_checksum   );

	if ( *checksum == *orig_checksum )
	{
		logInfo("checkChecksum -> Checksum is same as original = *checksum");
        	changeValueinICAT(*source, "ADMIN_Status" , "Checksum_ok" ) ;
                EUDATeiPIDeiChecksumMgmt(*source,"empty","false",bool("true"),0);
        	changeValueinICAT(*source, "ADMIN_Status" , "Archive_ok" ) ;
	}
	else
	{
		logInfo("checkChecksum -> Checksum (*checksum) is different than original (*orig_checksum)");
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


