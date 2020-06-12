#include "minirel.h"
#include "bufmgr.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"
#include "btfilescan.h"

//-------------------------------------------------------------------
// BTreeFileScan::~BTreeFileScan
//
// Input   : None
// Output  : None
// Purpose : Clean up the B+ tree scan.
//-------------------------------------------------------------------

BTreeFileScan::~BTreeFileScan()
{
    setPid(INVALID_PAGE);
    flag = "";
}


//-------------------------------------------------------------------
// BTreeFileScan::GetNext
//
// Input   : None
// Output  : rid  - record id of the scanned record.
//           key  - key of the scanned record
// Purpose : Return the next record from the B+-tree index.
// Return  : OK if successful, DONE if no more records to read.
//-------------------------------------------------------------------

Status
BTreeFileScan::GetNext(RecordID& rid, int& key)
{

    BTLeafPage *scanPage;
    PageID previousPid;
	RecordID outRid;
	Status status;

	if (scanPid == INVALID_PAGE) {
		return DONE;
	}
	PIN(scanPid, (Page *&)scanPage);
    status = scanPage->GetCurrent(key, outRid, scanRid);


    if(flag=="start")
    {
        if(status==DONE)
        {
            return DONE;
        }
        flag = "processing";
    }
    else if(flag != "delete")
    {
        if(status==DONE)
        {
            return DONE;
        }
        status = scanPage->GetNext(key, outRid, scanRid);
    }

	if (status == DONE) {

        previousPid = scanPid;
        setPid(scanPage->GetNextPage());

		if (scanPid == INVALID_PAGE) {
            UNPIN(previousPid, CLEAN);

			return DONE;
		}
        else
        {
			PIN(scanPid, (Page *&)scanPage);
			status = scanPage->GetFirst(key, outRid, scanRid);
            UNPIN(previousPid, CLEAN);
		}
	}
    UNPIN(scanPid, CLEAN);

	if ((highKey == nullptr) || (key <= *highKey)) {

        rid = outRid;
        currentKey = key;
        currentRid = outRid;
		return status;
	}
    else
    {
		return DONE;
	}

    return status;
}


//-------------------------------------------------------------------
// BTreeFileScan::DeleteCurrent
//
// Input   : None
// Output  : None
// Purpose : Delete the entry currently being scanned (i.e. returned
//           by previous call of GetNext())
// Return  : OK if successful, DONE if no more record to read.
//-------------------------------------------------------------------


Status
BTreeFileScan::DeleteCurrent()
{
    RecordID outRid;
    Status status;
    flag = "delete";
    status = btf->Delete(currentKey, currentRid);
    return status;
}
