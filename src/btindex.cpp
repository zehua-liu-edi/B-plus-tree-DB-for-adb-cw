#include <string.h>
#include "btindex.h"


//-------------------------------------------------------------------
// BTIndexPage::Insert
//
// Input   : key - value of the key to be inserted.
//           pageID - page id associated to that key.
// Output  : rid - record id of the (key, pageID) record inserted.
// Purpose : Insert the pair (key, pageID) into this index node.
// Return  : OK if insertion is succesfull, FAIL otherwise.
//-------------------------------------------------------------------

Status
BTIndexPage::Insert(const int key, const PageID pageID, RecordID& rid)
{
	IndexEntry entry;
	entry.key = key;
	entry.pid = pageID;

	Status s = SortedPage::InsertRecord((char *)&entry, sizeof(IndexEntry), rid);
	if (s != OK)
	{
		cerr << "Fail to insert record into IndexPage" << endl;
		return FAIL;
	}

	return OK;
}


//-------------------------------------------------------------------
// BTIndexPage::Delete
//
// Input   : key  - value of the key to be deleted.
// Output  : rid - record id of the (key, pageID) record deleted.
// Purpose : Delete the entry associated with key from this index node.
// Return  : OK if deletion is succesfull, FAIL otherwise. If FAIL is
//           returned, rid may contain garbage.
//-------------------------------------------------------------------

Status
BTIndexPage::Delete (const int key, RecordID& rid)
{
	// Scan through all the records in this page and find the
	// matching pair (key, dataRid).

	for (int i = numOfSlots - 1; i >= 0; i--)
	{
		IndexEntry* entry = (IndexEntry *)(data + slots[i].offset);
		if (entry->key == key)
		{
			// We delete it here.
			rid.pageNo = PageNo();
			rid.slotNo = i;
			Status s = SortedPage::DeleteRecord(rid);
            // printf("after delete %d, has num of slots %d\n", key, numOfSlots);
			return s;
		}
	}

	return FAIL;
}

//-------------------------------------------------------------------
// BTIndexPage::Search
//
// Input   : key - the key in child page find the related key in parent page.
// Output  : outKey - the related key in parent page.
//           pid - the page id related to the key
// Purpose : Find the first key smaller than child key in parent index page.
// Return  : OK if a record is returned,  DONE if no record exists on
//           this page.
//-------------------------------------------------------------------

Status BTIndexPage::Search (int key, PageID& pid, int& outKey)
{
	// Scan through all the records in this page and find the
	// matching pair (key, dataRid).

	for (int i = numOfSlots - 1; i >= 0; i--)
	{
		IndexEntry* entry = (IndexEntry *)(data + slots[i].offset);
		if (entry->key <= key)
		{
			// We delete it here.
			pid = entry->pid;
            outKey = entry->key;
			return OK;
		}
	}

    return DONE;
}

//-------------------------------------------------------------------
// BTIndexPage::leftSearch
//
// Input   : key - the key in child page find the related key in parent page.
// Output  : outKey - the related key in parent page.
//           pid - the page id related to the key
// Purpose : Find the first key bigger than child key in parent index page.
// Return  : OK if a record is returned,  DONE if no record exists on
//           this page.
//-------------------------------------------------------------------

Status BTIndexPage::leftSearch (int key, PageID& pid, int& outKey)
{

	for (int i = 0; i <= numOfSlots - 1; i++)
	{
		IndexEntry* entry = (IndexEntry *)(data + slots[i].offset);
		if (entry->key > key)
		{
			pid = entry->pid;
            outKey = entry->key;
			return OK;
		}
	}

    return DONE;
}

//-------------------------------------------------------------------
// BTIndexPage::changeKey
//
// Input   : newKey - the key to replace the previous key.
//           targetKey - the previous key in index page.
// Output  : None
// Purpose : Replace the old key in indexpage when ReDistribute
// Return  : OK if a record is returned,  DONE if no record exists on
//           this page.
//-------------------------------------------------------------------

Status BTIndexPage::changeKey (const int newKey, const int targetKey)
{
    for (int i = numOfSlots -1; i >= 0; i--) {

        IndexEntry* entry = (IndexEntry *)(data + slots[i].offset);

        if (targetKey == entry->key) {
            entry->key = newKey;
			memcpy(data+slots[i].offset, entry, sizeof(IndexEntry));
			return OK;
        }
    }
    return FAIL;
}

//-------------------------------------------------------------------
// BTIndexPage::GetFirst
//
// Input   : None
// Output  : rid - record id of the first entry
//           firstKey - pointer to the key value
//           firstPid - the page id
// Purpose : get the first pair (firstKey, firstPid) in the index page
//           and it's rid.
// Return  : OK if a record is returned,  DONE if no record exists on
//           this page.
//-------------------------------------------------------------------

Status
BTIndexPage::GetFirst(int& firstKey, PageID& firstPid, RecordID& rid)
{
	// Initialize the record id of the first (key, dataRid) pair.  The
	// first record is always at slot position 0, since SortedPage always
	// compact it's records.  We can also use HeapPage::FirstRecord here
	// but it is not neccessary.

	rid.pageNo = pid;
	rid.slotNo = 0;

	// If there are no record in this page, just return DONE.

	if (numOfSlots == 0)
	{
		rid.pageNo = INVALID_PAGE;
		rid.slotNo = INVALID_SLOT;
		return DONE;
	}

	// Otherwise, we just copy the record into key and dataRid,
	// and returned.  The record is located at offset slots[0].offset
	// from the beginning of data area (pointed to by member data in
	// HeapPage)

	IndexEntry entry;
	memcpy(&entry, (IndexEntry *)(data + slots[0].offset), sizeof(IndexEntry));
	firstKey = entry.key;
	firstPid = entry.pid;

	return OK;
}

//-------------------------------------------------------------------
// BTIndexPage::GetLast
//
// Input   : None
// Output  : rid - record id of the last entry
//           lastKey - pointer to the key value
//           lastPid - the page id
// Purpose : get the last pair (lastKey, lastPid) in the index page
//           and it's rid.
// Return  : OK if a record is returned,  DONE if no record exists on
//           this page.
//-------------------------------------------------------------------

Status
BTIndexPage::GetLast(int& lastKey, PageID& lastPid, RecordID& rid)
{

	rid.pageNo = pid;
	rid.slotNo = numOfSlots - 1;

	if (numOfSlots == 0)
	{
		rid.pageNo = INVALID_PAGE;
		rid.slotNo = INVALID_SLOT;
		return DONE;
	}

	IndexEntry entry;
	memcpy(&entry, (IndexEntry *)(data + slots[rid.slotNo].offset), sizeof(IndexEntry));
	lastKey = entry.key;
	lastPid = entry.pid;

	return OK;
}


//-------------------------------------------------------------------
// BTIndexPage::GetNext
//
// Input   : rid - record id of the current entry
// Output  : rid - record id of the next entry
//           nextKey - the key value of next entry
//           nextPid - the page id of next entry
// Purpose : Get the next pair (nextKey, nextPid) in the index page and
//           it's rid.
// Return  : OK if there is a next record, DONE if no more.  If DONE is
//           returned, rid is set to invalid.
//-------------------------------------------------------------------

Status
BTIndexPage::GetNext(int& nextKey, PageID& nextPid, RecordID& rid)
{
	// If we are at the end of records, return DONE.

	if (rid.slotNo + 1 >= numOfSlots)
	{
		rid.pageNo = INVALID_PAGE;
		rid.slotNo = INVALID_SLOT;
		return DONE;
	}

	// Increment the slotNo in rid to point to the next record in this
	// page.  We can do this for subclass of SorterPage since the records
	// in a sorted page is always compact.

	rid.slotNo++;

	// Otherwise, we just copy the record into key and dataRid,
	// and returned.  The record is located at offset
	// slots[rid.slotNo].offset from the beginning of data area
	// (pointed to by member data in HeapPage)

	IndexEntry entry;
	memcpy(&entry, (IndexEntry *)(data + slots[rid.slotNo].offset), sizeof(IndexEntry));
	nextKey = entry.key;
	nextPid = entry.pid;

	return OK;
}

//-------------------------------------------------------------------
// BTIndexPage::GetLeftLink
//
// Input   : None
// Output  : None
// Purpose : Return the page id of the page at the left of this page.
// Return  : The page id of the page at the left of this page.
//-------------------------------------------------------------------

PageID BTIndexPage::GetLeftLink()
{
	return GetPrevPage();
}


//-------------------------------------------------------------------
// BTIndexPage::SetLeftLink
//
// Input   : pageID - new left link
// Output  : None
// Purpose : Set the page id of the page at the left of this page.
// Return  : None
//-------------------------------------------------------------------

void BTIndexPage::SetLeftLink(PageID pageID)
{
	SetPrevPage(pageID);
}

//-------------------------------------------------------------------
// BTIndexPage::GetLeftLink
//
// Input   : None
// Output  : None
// Purpose : Return the page id of the page at the left of this page.
// Return  : The page id of the page at the left of this page.
//-------------------------------------------------------------------

PageID BTIndexPage::GetParentLink()
{
	return GetNextPage();
}


//-------------------------------------------------------------------
// BTIndexPage::SetLeftLink
//
// Input   : pageID - new left link
// Output  : None
// Purpose : Set the page id of the page at the left of this page.
// Return  : None
//-------------------------------------------------------------------

void BTIndexPage::SetParentLink(PageID pageID)
{
	SetNextPage(pageID);
}
