#include "minirel.h"
#include "bufmgr.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"
#include "btfilescan.h"

//-------------------------------------------------------------------
// BTreeFile::BTreeFile
//
// Input   : filename - filename of an index.
// Output  : returnStatus - status of execution of constructor.
//           OK if successful, FAIL otherwise.
// Purpose : If the B+ tree exists, open it.  Otherwise create a
//           new B+ tree index.
//-------------------------------------------------------------------

BTreeFile::BTreeFile (Status& returnStatus, const char* filename)
{
    Page * page ;
    Status getentry_state, newpage_state, addentry_state, pinpage_state;

    dbname = strcpy(new char[strlen(filename) + 1], filename);

    getentry_state = MINIBASE_DB->GetFileEntry(filename, rootPid);
    if(getentry_state==FAIL)
    {
        newpage_state = MINIBASE_BM -> NewPage ( rootPid , page );
        if(newpage_state!=OK)
        {
			rootPid = INVALID_PAGE;
			returnStatus = FAIL;
			return;
        }
        (( SortedPage *) page ) -> Init ( rootPid );
        addentry_state = MINIBASE_DB -> AddFileEntry ( filename , rootPid );
        if(addentry_state!=OK)
        {
			rootPid = INVALID_PAGE;
            filename = NULL;
			returnStatus = FAIL;
			return;
        }
        (( SortedPage *) page ) -> SetType (LEAF_NODE);
    }
    else
    {
        pinpage_state = MINIBASE_BM -> PinPage ( rootPid , page );
        if(pinpage_state!=OK)
        {
			rootPid = INVALID_PAGE;
            filename = NULL;
			returnStatus = FAIL;
			return;
        }
        MINIBASE_BM->UnpinPage (rootPid, CLEAN);
    }

    returnStatus = OK;

}


//-------------------------------------------------------------------
// BTreeFile::~BTreeFile
//
// Input   : None
// Output  : None
// Purpose : Clean Up
//-------------------------------------------------------------------

BTreeFile::~BTreeFile()
{
    Status status;
    rootPid = INVALID_PAGE;
    delete [] dbname;
}


void BTreeFile::clearPath()
{
    path = stack<PageID>();
}

//-------------------------------------------------------------------
// BTreeFile::DestroyFile
//
// Input   : None
// Output  : None
// Return  : OK if successful, FAIL otherwise.
// Purpose : Delete the entire index file by freeing all pages allocated
//           for this BTreeFile.
//-------------------------------------------------------------------

Status
BTreeFile::DestroyFile()
{
    Status status= OK;
    PageID rootPageID;
    SortedPage* rootPage;
    short nType;
    if (rootPid == INVALID_PAGE)
    {
		status = MINIBASE_DB->DeleteFileEntry(dbname);
        if(status==OK)
            cout << "  Success" << endl;
		return status;
	}
	else
    {
		PIN(rootPid,(Page *&)rootPage);
		if( rootPage->GetType()==LEAF_NODE) {
            UNPIN(rootPid, CLEAN);
			FREEPAGE(rootPid);
		}
		else {
            UNPIN(rootPid, CLEAN);
			status=DestroyAll(rootPid);
            if(status==FAIL)
                return status;
		}
	}

	rootPid = INVALID_PAGE;
	status = MINIBASE_DB->DeleteFileEntry(dbname);
    cout << "  Success" << endl;
	return status;
}

//-------------------------------------------------------------------
// BTreeFile::DestroyAll
//
// Input   : None
// Output  : None
// Return  : OK if successful, FAIL otherwise.
// Purpose : Recursively delete all the nodes
//-------------------------------------------------------------------

Status BTreeFile::DestroyAll(PageID pageID)
{
	if ( pageID == INVALID_PAGE ) {
    	return FAIL;
	}

	SortedPage* page = nullptr;
	PIN(pageID, page);
	NodeType type = (NodeType) page->GetType();

	if (type == INDEX_NODE)
	{
		PageID curPageID = ((BTIndexPage*&)page)->GetLeftLink();
		DestroyAll(curPageID);

		RecordID curRid;
		int key;
		Status s = ((BTIndexPage*&)page)->GetFirst(key, curPageID, curRid);

		while (s != DONE)
		{
			DestroyAll(curPageID);
			s = ((BTIndexPage*&)page)->GetNext(key, curPageID, curRid);
		}

        UNPIN(pageID, CLEAN);

        return s;
	}

    UNPIN(pageID, CLEAN);

    FREEPAGE(pageID);

	return OK;
}

//-------------------------------------------------------------------
// BTreeFile::Insert
//
// Input   : key - the value of the key to be inserted.
//           rid - RecordID of the record to be inserted.
// Output  : None
// Return  : OK if successful, FAIL otherwise.
// Purpose : Insert an index entry with this rid and key.
// Note    : If the root didn't exist, create it.
//-------------------------------------------------------------------


Status
BTreeFile::Insert(const int key, const RecordID rid)
{
    Status status;
    SortedPage * rootPage ;
    BTLeafPage * leafpage ;
    BTIndexPage * indexpage;
    RecordID outRid;
    PageID leafPid, indexPid, outPid;
    clearPath();

    short type;
    if (rootPid == INVALID_PAGE)
    {
        Page * page ;
        MINIBASE_BM -> NewPage ( rootPid , (Page *&)rootPage);
        rootPage -> Init ( rootPid );
        rootPage -> SetType (LEAF_NODE);
    }
    else
    {
        PIN(rootPid, (Page *&)rootPage);
    }

    type = rootPage->GetType();

    /* If root is leaf page, insert into root. Or search for the node to insert*/
    if(type == LEAF_NODE)
    {
        leafpage = (BTLeafPage* &)rootPage;
        leafPid = rootPid;
        PageID newRootPageID;

        if(leafpage -> AvailableSpace () >= INSERTSIZE)
        {
            leafpage -> Insert ( key , rid , outRid );

            UNPIN(leafPid, DIRTY);
        }
        else
        {
            UNPIN(leafPid, DIRTY);
            SplitLeafNode(leafPid, rootPid, key, rid);

        }
    }
    else
    {
        indexPid = rootPid;
        UNPIN(rootPid, CLEAN);
        IndexSearch(indexPid, key, rid, outPid, "insert");
    }


    return OK;
}

//-------------------------------------------------------------------
// BTreeFile::SplitLeafNode
//
// Input   : key - the value of the key to be inserted.
//           rid - RecordID of the record to be inserted.
//           leafPageID - the leaf page to be split
//           newRootPageID - the parent page id for the leaf page
// Output  : None
// Return  : OK if successful, FAIL otherwise.
// Purpose : Split the leaf page when it is full.
// Note    : A index key will be inserted into parent page.
//           A new leaf page will be created.
//-------------------------------------------------------------------

Status BTreeFile::SplitLeafNode(PageID leafPageID, PageID& parentPageID, const int key, const RecordID rid)
{
    BTIndexPage* parentPage;
    BTLeafPage *leafPage;
    PageID newLeafPageID;
    BTLeafPage* newLeafPage;
    int firstKey;
    RecordID firstRid, outRid;
    Status pin_status = FAIL;
    Status left_insert = FAIL;

    if(leafPageID == rootPid)
    {
        MINIBASE_BM ->NewPage(parentPageID, (Page *&)parentPage);
    	parentPage->SetType(INDEX_NODE);
    	parentPage->Init(parentPageID);
    }
    else
    {
        MINIBASE_BM->PinPage(parentPageID, (Page *&)parentPage);
    }

    MINIBASE_BM ->NewPage(newLeafPageID, (Page *&)newLeafPage);
	newLeafPage->SetType(LEAF_NODE);
	newLeafPage->Init(newLeafPageID);

    PIN(leafPageID, (Page *&)leafPage);
    int totalSize = (HEAPPAGE_DATA_SIZE - leafPage->AvailableSpace())/INSERTSIZE;

    for (int i = 0; i < totalSize/2; i++) {
       leafPage -> GetLast(firstKey, firstRid, outRid);
       newLeafPage -> Insert ( firstKey , firstRid , outRid );
       leafPage -> Delete(firstKey, firstRid, outRid);
    }

    newLeafPage -> GetFirst(firstKey,  firstRid, outRid);

    if(key>firstKey)
    {
        newLeafPage-> Insert ( key , rid , outRid );
    }
    else
    {
        leafPage-> Insert ( key , rid , outRid );
    }

    if(parentPage-> AvailableSpace () >= INDEXENTRYSIZE)
    {
        parentPage -> Insert ( firstKey , newLeafPageID , outRid );
        if(outRid.slotNo==0)
            parentPage->SetLeftLink(leafPageID);
        UNPIN(parentPageID, DIRTY);
    }
    else
    {
        UNPIN(parentPageID, DIRTY);
        SplitIndex(parentPageID, firstKey, newLeafPageID);
    }

    if(rootPid == leafPageID)
    {
        rootPid = parentPageID;
    }


    leafPage->SetNextPage(newLeafPageID);
    newLeafPage->SetPrevPage(leafPageID);

    UNPIN(leafPageID, DIRTY);
	UNPIN(newLeafPageID, DIRTY);

    return OK;
}

//-------------------------------------------------------------------
// BTreeFile::SplitIndex
//
// Input   : key - the value of the key to be inserted.
//           pid - PageID of the record to be inserted.
//           prevIndexPageID - the index page to be split
// Output  : None
// Return  : OK if successful, FAIL otherwise.
// Purpose : Split the index page when it is full.
// Note    : A index key will be inserted into parent page.
//           A new index page will be created.
//-------------------------------------------------------------------

Status BTreeFile::SplitIndex(PageID prevIndexPageID, const int key, const PageID pid)
{
    SortedPage *test;
    BTIndexPage* prevIndexPage, *parentPage;
    BTIndexPage* newIndexPage;
    PageID newIndexPageID, parentPageID, lastPid, firstPid;
    RecordID outRid;
    int lastKey, firstKey;

    path.pop();
    if(prevIndexPageID == rootPid)
    {
        MINIBASE_BM -> NewPage(parentPageID, (Page *&)parentPage);
    	parentPage -> SetType(INDEX_NODE);
    	parentPage -> Init(parentPageID);
        rootPid = parentPageID;
        parentPage -> SetLeftLink(prevIndexPageID);
    }
    else
    {
        parentPageID = path.top();

        PIN(parentPageID, (Page *&)parentPage);
    }

    MINIBASE_BM ->NewPage(newIndexPageID, (Page *&)newIndexPage);
    newIndexPage->SetType(INDEX_NODE);
    newIndexPage->Init(newIndexPageID);

    PIN(prevIndexPageID, (Page *&)prevIndexPage);

    int totalSize = (HEAPPAGE_DATA_SIZE - prevIndexPage->AvailableSpace())/INDEXENTRYSIZE;

    for (int i = 0; i < totalSize/2; i++) {
       prevIndexPage -> GetLast(lastKey, lastPid, outRid);
       newIndexPage -> Insert ( lastKey , lastPid , outRid );
       prevIndexPage -> Delete(lastKey, outRid);
    }

    newIndexPage -> GetFirst(firstKey,  firstPid, outRid);

    if(key>firstKey)
        newIndexPage-> Insert ( key , pid , outRid );
    else
        prevIndexPage-> Insert ( key , pid , outRid );

    newIndexPage -> GetFirst(firstKey,  firstPid, outRid);

    // If parent page has enough space, insert without split.
    if(parentPage->AvailableSpace()>=INDEXENTRYSIZE)
    {
        parentPage-> Insert ( firstKey , newIndexPageID , outRid );
        newIndexPage-> Delete(firstKey, outRid);

        newIndexPage -> SetLeftLink(firstPid);
        UNPIN(parentPageID, DIRTY);
        UNPIN(prevIndexPageID, DIRTY);
        UNPIN(newIndexPageID, DIRTY);
        return OK;
    }
    // Recursively split the parent page if it's full.
    else
    {
        newIndexPage-> Delete(firstKey, outRid);
        newIndexPage -> SetLeftLink(firstPid);
        UNPIN(parentPageID, DIRTY);
        UNPIN(prevIndexPageID, DIRTY);
        UNPIN(newIndexPageID, DIRTY);
        SplitIndex(parentPageID, firstKey, newIndexPageID);
    }

    return OK;

}

//-------------------------------------------------------------------
// BTreeFile::LeafInsert
//
// Input   : key - the value of the key to be inserted.
//           rid - RecordID of the record to be inserted.
//           childPid - the id of leaf page to be inserted
//           indexPid - the id of parent page of inserted leaf page
// Output  : None
// Return  : OK if successful, FAIL otherwise.
// Purpose : Insert into leaf page. If it's full, split it.
//-------------------------------------------------------------------

Status BTreeFile::LeafInsert(PageID childPid, PageID indexPid, const int key, const RecordID rid)
{
    BTLeafPage *childLeafPage;
    RecordID outRid;

    PIN(childPid, (Page *&)childLeafPage);

    if(childLeafPage -> AvailableSpace () >= INSERTSIZE)
    {

        childLeafPage-> Insert ( key , rid , outRid );

        UNPIN(childPid, DIRTY);
    }
    else
    {
        UNPIN(childPid, DIRTY);
        SplitLeafNode(childPid, indexPid, key, rid);
    }
    return OK;
}

//-------------------------------------------------------------------
// BTreeFile::IndexSearch
//
// Input   : key - the value of the key to be inserted.
//           rid - RecordID of the record to be inserted.
//           indexPid - the latest pid this recursive method
//           flag - the operation to do (search, insert or delete)
// Output  : outPid - the Pid of page contains the key. Used in openscan.
// Return  : OK if successful, FAIL otherwise.
// Purpose : Core function in this work. Recursively go through the Tree
//           to achieve insert/search/delete operations based on flag.
// Note    : path is a stack declared in BTreeFile class which is to store the
//           index nodes the search has scaned (parents). Each time
//           scan an index page, its pid will be pushed into stack
//           so that the top of the stack is always the parent of
//           current page.
//-------------------------------------------------------------------

Status BTreeFile::IndexSearch(PageID indexPid, const int key, const RecordID rid, PageID& outPid, string flag)
{
    int firstKey, nextKey, outKey;
    PageID firstPid, childPid;
    RecordID firstRid, nextRid, outRid;
    BTLeafPage *childLeafPage;
    BTIndexPage *childIndexPage;
    SortedPage *childPage;
    BTIndexPage *indexpage;
    short type;
    Status status;
    PIN(indexPid,(Page *&)indexpage);
    path.push(indexPid);

    indexpage -> GetFirst(firstKey, firstPid, firstRid);

    if(key<firstKey)
    {
        childPid = indexpage->GetLeftLink();

    }
    else
    {
        status = OK;
        while (status!=DONE) {
            if(key==firstKey)
            {
                childPid = firstPid;
                break;
            }
            if(key<firstKey)
                break;
            childPid = firstPid;
            status = indexpage->GetNext(firstKey, firstPid, firstRid);
        }
    }


    PIN(childPid, (Page *&)childPage);

    type = childPage->GetType();

    if(type == LEAF_NODE)
    {

        if(flag == "insert")
        {
            UNPIN(childPid, CLEAN);
            UNPIN(indexPid, CLEAN);
            status = LeafInsert(childPid, indexPid, key, rid);
            return status;
        }
        if(flag == "search")
        {
            path.push(childPid);
            outPid = childPid;
            UNPIN(childPid, CLEAN);
            UNPIN(indexPid, CLEAN);
            return OK;
        }
        if(flag == "delete")
        {
            childLeafPage = (BTLeafPage *)childPage;

            childLeafPage->Delete(key, rid, outRid);

            UNPIN(childPid,DIRTY);
            if(childLeafPage-> AvailableSpace () > HEAPPAGE_DATA_SIZE / 2)
            {
                status = ReDistributeMerge(childPid);
            }

        }
    }
    else
    {
        UNPIN(childPid,CLEAN);
        status = IndexSearch(childPid, key, rid, outPid, flag);
    }

    bool right = false;

    if(flag == "delete")
    {
        if(!indexpage->IsAtLeastHalfFull()&&(indexPid!=rootPid))
        {
            path.pop();
            PageID siblingIndexID = getSibilingIndex(indexPid, path.top(), right);
            IndexReDistributeMerge(indexPid, indexpage, siblingIndexID, right);
            UNPIN(indexPid, DIRTY);

        }
        else
        {
            if((indexpage->GetNumOfRecords()==0)&&(indexPid==rootPid))
            {
                rootPid = indexpage->GetLeftLink();
            }

            path.pop();

            UNPIN(indexPid, CLEAN);
        }
    }
    else
        UNPIN(indexPid, CLEAN);

    return OK;
}

//-------------------------------------------------------------------
// BTreeFile::getSibilingIndex
//
// Input   : childPid - the pid ask for redistribute
//           parentPid - the parent pid of page who asks for redistribute
// Output  : right - the position of siblingPage, right is true, left is false
// Return  : PageID - the PageID of the siblingPage
// Purpose : Find the sibling index page by the parent
//-------------------------------------------------------------------

PageID BTreeFile::getSibilingIndex(PageID childPid, PageID parentPid, bool &right)
{
    int firstKey, nextKey, lastKey;
    PageID firstPid, nextPid, lastPid, prevPid;
    RecordID firstRid, nextRid, lastRid;
    Status status = OK;
    bool find = false;

    BTIndexPage *parentPage;
    PIN(parentPid, (Page *&)parentPage);
    parentPage -> GetFirst(firstKey,  firstPid, firstRid);
    // if(childPid == parentPage->GetLeftLink())
    //     return firstPid;
    if(childPid == firstPid)
    {
        status = parentPage->GetNext(nextKey, nextPid, firstRid);
        if(status==OK)
        {
            right = true;
            return nextPid;
        }
        else
        {
            right = false;
            return parentPage->GetLeftLink();
        }
    }
    else
    {
        while (status !=DONE)
        {

            prevPid = nextPid;
            status = parentPage->GetNext(nextKey, nextPid, firstRid);

            if(childPid == nextPid)
            {
                status = parentPage->GetNext(nextKey, nextPid, firstRid);
                if(status==OK)
                {
                    right = true;
                    return nextPid;
                }
                else
                {
                    right = false;
                    return prevPid;
                }
            }
        }
    }
    right = true;
    return firstPid;
}

//-------------------------------------------------------------------
// BTreeFile::Delete
//
// Input   : key - the value of the key to be deleted.
//           rid - RecordID of the record to be deleted.
// Output  : None
// Return  : OK if successful, FAIL otherwise.
// Purpose : Delete an index entry with this rid and key.
// Note    : If the root becomes empty, delete it.
//-------------------------------------------------------------------

Status
BTreeFile::Delete(const int key, const RecordID rid)
{
    // TODO: add your code here
    Status status;
    SortedPage * rootPage ;
    BTLeafPage * leafpage ;
    BTIndexPage * indexpage;
    RecordID outRid;
    PageID leafPid, indexPid, outPid;
    clearPath();

    short type;
    if (rootPid == INVALID_PAGE)
    {
        return DONE;
    }
    else
    {
        PIN(rootPid, (Page *&)rootPage);
    }

    type = rootPage->GetType();

    if (type == LEAF_NODE) {

		BTLeafPage *leafPage = (BTLeafPage *)rootPage;

		status = leafPage->Delete(key, rid, outRid);
        UNPIN(rootPid,CLEAN);
		if (leafPage->GetNumOfRecords() == 0) {
			rootPid = INVALID_PAGE;
		}

		return status;
	}
    else
    {
        UNPIN(rootPid,CLEAN);
        status =  IndexSearch(rootPid, key, rid, outPid, "delete");
	}

    return status;
}

//-------------------------------------------------------------------
// BTreeFile::ReDistributeMerge
//
// Input   : childPid - leaf page id requires ReDistribute and merge
// Output  : None
// Return  : OK if successful, FAIL otherwise.
// Purpose : If ReDistribute is not available, call the merge function.
// Note    : This is function for leaf page.
//-------------------------------------------------------------------

Status BTreeFile::ReDistributeMerge(PageID childPid)
{
    BTLeafPage *childLeafPage;
    BTLeafPage *leftSiblingPage, *rightSiblingPage, *siblingLeafPage;
    PageID leftSiblingPageID, rightSiblingPageID, curPageID, lastPid;

    int firstKey, lastKey, outKey;
    RecordID firstRid, outRid, curRid;

    PageID indexPageID = path.top();

    BTIndexPage *indexPage;

    PIN(indexPageID, (Page *&)indexPage);
    PIN(childPid, (Page *&)childLeafPage);

    int totalSlots = HEAPPAGE_DATA_SIZE/INSERTSIZE;
    int selfSlots = (HEAPPAGE_DATA_SIZE - childLeafPage-> AvailableSpace ())/INSERTSIZE;

    leftSiblingPageID = childLeafPage->GetPrevPage();
    rightSiblingPageID = childLeafPage->GetNextPage();

    PageID tempID = indexPage -> GetLeftLink();
    indexPage->GetLast(lastKey, lastPid, outRid);

    if(childPid != lastPid)
    {

        PIN(rightSiblingPageID, (Page *&)rightSiblingPage);

        int siblingSlots = (HEAPPAGE_DATA_SIZE - rightSiblingPage-> AvailableSpace ())/INSERTSIZE;

        if(rightSiblingPage-> AvailableSpace () <= HEAPPAGE_DATA_SIZE / 2)
        {

            rightSiblingPage->GetFirst(firstKey,  firstRid, outRid);
            indexPage -> Search(firstKey, curPageID, outKey);
            indexPage -> Delete(outKey, outRid);

            while (( childLeafPage->AvailableSpace () > HEAPPAGE_DATA_SIZE / 2)
            &&( rightSiblingPage -> AvailableSpace () <= HEAPPAGE_DATA_SIZE / 2))
            {
                rightSiblingPage->GetFirst(firstKey,  firstRid, outRid);
                childLeafPage -> Insert(firstKey,  firstRid, outRid);
                rightSiblingPage->Delete(firstKey, firstRid, outRid);
            }

            rightSiblingPage->GetFirst(firstKey,  firstRid, outRid);
            indexPage -> Insert(firstKey,  rightSiblingPageID, outRid);


            if(( childLeafPage -> AvailableSpace () <= HEAPPAGE_DATA_SIZE / 2)
            &&( rightSiblingPage -> AvailableSpace () <= HEAPPAGE_DATA_SIZE / 2))
            {
                UNPIN(rightSiblingPageID, DIRTY);
                UNPIN(childPid, DIRTY);
                UNPIN(indexPageID, DIRTY);
                return OK;
            }
        }
        if((siblingSlots+selfSlots)<=totalSlots)
            MergeLeaf(childLeafPage, rightSiblingPage, indexPage, "right");

        UNPIN(rightSiblingPageID, DIRTY);
        UNPIN(childPid, DIRTY);
        UNPIN(indexPageID, DIRTY);
        return OK;
    }
    else
    {
            PIN(leftSiblingPageID, (Page *&)leftSiblingPage);
            int siblingSlots = (HEAPPAGE_DATA_SIZE - leftSiblingPage-> AvailableSpace ())/INSERTSIZE;
            if(leftSiblingPage-> AvailableSpace () <= HEAPPAGE_DATA_SIZE / 2)
            {

                childLeafPage->GetFirst(firstKey,  firstRid, outRid);
                indexPage -> Search(firstKey, curPageID, outKey);
                indexPage -> Delete(outKey, outRid);
                while (( childLeafPage -> AvailableSpace () > HEAPPAGE_DATA_SIZE / 2)
                &&( leftSiblingPage -> AvailableSpace () <= HEAPPAGE_DATA_SIZE / 2))
                {
                    leftSiblingPage->GetLast(firstKey,  firstRid, outRid);
                    childLeafPage -> Insert(firstKey,  firstRid, outRid);
                    leftSiblingPage->Delete(firstKey, firstRid, outRid);
                }
                childLeafPage->GetFirst(firstKey,  firstRid, outRid);
                indexPage -> Insert(firstKey,  childPid, outRid);
                if(( childLeafPage -> AvailableSpace () <= HEAPPAGE_DATA_SIZE / 2)
                &&( leftSiblingPage -> AvailableSpace () <= HEAPPAGE_DATA_SIZE / 2))
                {
                    UNPIN(leftSiblingPageID, DIRTY);
                    UNPIN(childPid, DIRTY);
                    UNPIN(indexPageID, DIRTY);
                    return OK;
                }
            }
            if((siblingSlots+selfSlots)<=totalSlots)
                MergeLeaf(childLeafPage, leftSiblingPage, indexPage, "left");
            leftSiblingPage->GetLast(firstKey,  firstRid, outRid);

            UNPIN(leftSiblingPageID, DIRTY);
            UNPIN(childPid, DIRTY);
            UNPIN(indexPageID, DIRTY);
            return OK;

    }


    return DONE;
}

//-------------------------------------------------------------------
// BTreeFile::IndexReDistributeMerge
//
// Input   : childPid - index page id requires ReDistribute and merge
//           childIndexPage - index page requires ReDistribute and merge
//           siblingIndexID - sibling page id of target index page
//           right - the position of the sibling page
// Output  : None
// Return  : OK if successful, FAIL otherwise.
// Purpose : If ReDistribute is not available, call the merge function.
// Note    : This is function for index page.
//-------------------------------------------------------------------

Status BTreeFile::IndexReDistributeMerge(PageID childPid, BTIndexPage *childIndexPage, PageID siblingIndexID, bool right)
{
    BTIndexPage *siblingPage;
    PageID leftSiblingPageID, rightSiblingPageID, curPageID, lastPid;

    int firstKey, lastKey, parentKey;
    RecordID firstRid, outRid;
    PageID firstPid, parentPid;

    bool leftDistribute = false;
    bool rightDistribute = false;

    PageID indexPageID = path.top();
    BTIndexPage *indexPage;

    PIN(indexPageID, (Page *&)indexPage);
    PIN(siblingIndexID, (Page *&)siblingPage);

    int totalSlots = HEAPPAGE_DATA_SIZE/INDEXENTRYSIZE;
    int siblingSlots = (HEAPPAGE_DATA_SIZE - siblingPage-> AvailableSpace ())/INDEXENTRYSIZE;
    int selfSlots = (HEAPPAGE_DATA_SIZE - childIndexPage-> AvailableSpace ())/INDEXENTRYSIZE;

    if(right)
    {
        if(siblingPage-> AvailableSpace () <= HEAPPAGE_DATA_SIZE / 2)
        {

            while (( childIndexPage -> AvailableSpace () > HEAPPAGE_DATA_SIZE / 2)
            &&( siblingPage -> AvailableSpace () <= HEAPPAGE_DATA_SIZE / 2))
            {
                siblingPage->GetFirst(firstKey,  firstPid, outRid);
                indexPage->Search(firstKey, parentPid, parentKey);
                childIndexPage -> Insert(parentKey,  siblingPage->GetLeftLink(), outRid);
                siblingPage -> SetLeftLink(firstPid);
                indexPage -> changeKey(firstKey, parentKey);
                siblingPage->Delete(firstKey, outRid);
            }


            if(( childIndexPage -> AvailableSpace () <= HEAPPAGE_DATA_SIZE / 2)
            &&( siblingPage -> AvailableSpace () <= HEAPPAGE_DATA_SIZE / 2))
            {
                UNPIN(siblingIndexID, DIRTY);
                UNPIN(indexPageID, DIRTY);
                return OK;
            }
        }
        if((siblingSlots+selfSlots)<totalSlots)
            MergeIndex(childIndexPage, siblingPage, indexPage, "right");
        UNPIN(siblingIndexID, DIRTY);
        UNPIN(indexPageID, DIRTY);
        return OK;
    }
    else
    {
            if(siblingPage-> AvailableSpace () <= HEAPPAGE_DATA_SIZE / 2)
            {

                while (( childIndexPage -> AvailableSpace () > HEAPPAGE_DATA_SIZE / 2)
                &&( siblingPage -> AvailableSpace () <= HEAPPAGE_DATA_SIZE / 2))
                {

                    siblingPage->GetLast(firstKey,  firstPid, outRid);

                    indexPage->leftSearch(firstKey, parentPid, parentKey);

                    childIndexPage -> Insert(parentKey,  childIndexPage->GetLeftLink(), outRid);
                    childIndexPage -> SetLeftLink(firstPid);

                    indexPage -> changeKey(firstKey, parentKey);
                    // indexPage->Delete(parentKey, outRid);
                    // indexPage-> Insert(firstKey,  childPid, outRid);
                    // indexPage-> Insert(firstKey,  childPid, outRid);
                    siblingPage->Delete(firstKey, outRid);
                }

                if(( childIndexPage -> AvailableSpace () <= HEAPPAGE_DATA_SIZE / 2)
                &&( siblingPage -> AvailableSpace () <= HEAPPAGE_DATA_SIZE / 2))
                {

                    UNPIN(siblingIndexID, DIRTY);
                    UNPIN(indexPageID, DIRTY);
                    return OK;
                }
            }

            if((siblingSlots+selfSlots)<totalSlots)
                MergeIndex(childIndexPage, siblingPage, indexPage, "left");
            UNPIN(siblingIndexID, DIRTY);
            UNPIN(indexPageID, DIRTY);

            return OK;
    }
    return DONE;
}

//-------------------------------------------------------------------
// BTreeFile::MergeIndex
//
// Input   : childIndexPage - index page that needs to merge
//           siblingPage - sibling page of index page
//           siblingIndexID - sibling page id of target index page
//           flag - the position of the sibling page
// Output  : None
// Return  : OK if successful, FAIL otherwise.
// Purpose : Merge the target page and its siblingPage.
// Note    : This is function for index page.
//-------------------------------------------------------------------

Status BTreeFile::MergeIndex(BTIndexPage *childIndexPage, BTIndexPage *siblingPage, BTIndexPage *indexPage, string flag)
{
    int firstKey, indexKey, parentKey;
    RecordID firstRid, outRid, indexRid;
    PageID firstPid, indexPid, parentPid;
    PageID leftMostID;

    if(flag == "right")
    {
        siblingPage->GetFirst(indexKey,  indexPid, indexRid);
        indexPage->Search(indexKey, parentPid, parentKey);
        childIndexPage -> Insert(parentKey,  siblingPage->GetLeftLink(), outRid);
    }
    else
    {
        childIndexPage->GetFirst(indexKey,  indexPid, indexRid);
        indexPage->Search(indexKey, parentPid, parentKey);
        siblingPage->Insert(parentKey,  childIndexPage->GetLeftLink(), outRid);
    }
    indexPage ->Delete(parentKey, indexRid);
    if(flag == "right")
    {
        while(siblingPage->GetNumOfRecords() > 0)
        {
            siblingPage->GetFirst(firstKey,  firstPid, outRid);
            childIndexPage->Insert(firstKey,  firstPid, outRid);
            siblingPage->Delete(firstKey, outRid);
        }

    }
    else
    {
        while(childIndexPage->GetNumOfRecords() > 0)
        {

            childIndexPage->GetFirst(firstKey,  firstPid, outRid);
            siblingPage->Insert(firstKey,  firstPid, outRid);
            childIndexPage->Delete(firstKey, outRid);
        }

    }

    return OK;
}

//-------------------------------------------------------------------
// BTreeFile::MergeLeaf
//
// Input   : childLeafPage - leaf page that needs to merge
//           siblingPage - sibling page of leaf page
//           indexPage - parent page id of target leaf page
//           flag - the position of the sibling page
// Output  : None
// Return  : OK if successful, FAIL otherwise.
// Purpose : Merge the target page and its siblingPage.
// Note    : This is function for leaf page.
//-------------------------------------------------------------------

Status BTreeFile::MergeLeaf(BTLeafPage *childLeafPage, BTLeafPage *siblingPage, BTIndexPage *indexPage, string flag)
{
    int firstKey, indexKey, leafKey;
    RecordID firstRid, outRid, leafRid, indexRid;
    PageID leftMostID, indexPid;

    if(flag == "right")
    {
        siblingPage->GetFirst(leafKey,  leafRid, outRid);
        indexPage -> Search(leafKey, indexPid, indexKey);
    }
    else
    {
        childLeafPage->GetFirst(leafKey,  leafRid, outRid);
        indexPage -> Search(leafKey, indexPid, indexKey);
    }

    if(flag == "right")
    {
        while(siblingPage->GetNumOfRecords() > 0)
        {
            siblingPage->GetFirst(firstKey,  firstRid, outRid);
            childLeafPage->Insert(firstKey,  firstRid, outRid);
            siblingPage->Delete(firstKey, firstRid, outRid);
        }
    }
    else
    {
        while(childLeafPage->GetNumOfRecords() > 0)
        {
            childLeafPage->GetFirst(firstKey,  firstRid, outRid);
            siblingPage->Insert(firstKey,  firstRid, outRid);
            childLeafPage->Delete(firstKey, firstRid, outRid);
        }
    }


    if(flag == "right")
    {
        PageID nextPageID = siblingPage->GetNextPage();
        childLeafPage->SetNextPage(nextPageID);
        indexPage ->Delete(indexKey, indexRid);
    }
    else
    {
        PageID nextPageID = childLeafPage->GetNextPage();
        siblingPage->SetNextPage(nextPageID);
        indexPage ->Delete(indexKey, indexRid);
    }
    childLeafPage ->GetLast(firstKey,  indexRid, outRid);

    return OK;
}

//-------------------------------------------------------------------
// BTreeFile::OpenScan
//
// Input   : lowKey, highKey - pointer to keys, indicate the range
//                             to scan.
// Output  : None
// Return  : A pointer to IndexFileScan class.
// Purpose : Initialize a scan.
// Note    : Usage of lowKey and highKey :
//
//           lowKey      highKey      range
//			 value	     value
//           --------------------------------------------------
//           nullptr     nullptr      whole index
//           nullptr     !nullptr     minimum to highKey
//           !nullptr    nullptr      lowKey to maximum
//           !nullptr    =lowKey  	  exact match
//           !nullptr    >lowKey      lowKey to highKey
//-------------------------------------------------------------------

IndexFileScan*
BTreeFile::OpenScan(const int* lowKey, const int* highKey)
{
    RecordID rid;
    int key, tempkey;
    RecordID outRid, tempRid;

	PageID rootPageID, outPid;
	PageID startPageID;
    BTLeafPage *startPage;
    clearPath();

	BTreeFileScan* scan=new BTreeFileScan();


	scan->setFlag("start");
    scan->setHighKey(highKey);
    scan->setLowKey(lowKey);
    scan->setBtf(this);

	if (rootPid == INVALID_PAGE){

		scan->setPid(INVALID_PAGE);
		return scan;
	}

	if (rootPid!=INVALID_PAGE) {

		if (lowKey == nullptr)
        {
			startPageID = GetLeftLeaf(rootPid);
		}
        else
        {
            scan->setLowKey(lowKey);
			if (IndexSearch(rootPid, *lowKey, rid, outPid, "search") != OK)
            {
                return scan;
            }
            scan->setPid(outPid);

            startPageID = outPid;
		}



		MINIBASE_BM->PinPage(startPageID, (Page *&)startPage);
		if (startPage->GetNumOfRecords() <= 0) {
			return scan;
		}

		startPage->GetFirst(key, outRid, rid);

		if (lowKey != nullptr)
        {
            startPage->GetLast(tempkey, outRid, tempRid);
            if(*lowKey>tempkey)
            {
                scan->setPid(INVALID_PAGE);

                return scan;
            }
            else
            {
                while(*lowKey > key)
                {
    				startPage->GetNext(key, outRid, rid);
    			}
            }

		}
        else
        {
            scan->setLowKey(&key);
            scan->setPid(startPageID);
        }
		scan->setRid(rid);


		MINIBASE_BM->UnpinPage(startPageID, CLEAN);
	}

	return scan;
}

//-------------------------------------------------------------------
// BTreeFile::GetLeftLeaf
//
// Input   : parentPid - root of the tree to get left most page.
// Output  : None
// Return  : OK if successful, FAIL otherwise.
// Purpose : Recursively find the leftmost page.
// Note    : This is used in scan. When input -1 as lowkey, we need to set it leftmost.
//-------------------------------------------------------------------

PageID BTreeFile::GetLeftLeaf(PageID parentPid)
{
    SortedPage *parentPage;
    PIN(parentPid, (Page *&)parentPage);
    short type = parentPage->GetType();

    if(type==LEAF_NODE)
    {
        UNPIN(parentPid, CLEAN);
        return parentPid;
    }

    PageID leftChild = parentPage->GetPrevPage();
    UNPIN(parentPid, CLEAN);
    return GetLeftLeaf(leftChild);

}

//-------------------------------------------------------------------
// BTreeFile::PrintTree
//
// Input   : pageID - root of the tree to print.
// Output  : None
// Return  : None
// Purpose : Print out the content of the tree rooted at pid.
//-------------------------------------------------------------------

Status
BTreeFile::PrintTree(PageID pageID)
{
	if ( pageID == INVALID_PAGE ) {
    	return FAIL;
	}

	SortedPage* page = nullptr;
	PIN(pageID, page);

	NodeType type = (NodeType) page->GetType();
	if (type == INDEX_NODE)
	{
		BTIndexPage* index = (BTIndexPage *) page;
		PageID curPageID = index->GetLeftLink();
		PrintTree(curPageID);

		RecordID curRid;
		int key;
		Status s = index->GetFirst(key, curPageID, curRid);

		while (s != DONE)
		{
			PrintTree(curPageID);
			s = index->GetNext(key, curPageID, curRid);
		}
	}

	UNPIN(pageID, CLEAN);

	PrintNode(pageID);

	return OK;
}

//-------------------------------------------------------------------
// BTreeFile::PrintNode
//
// Input   : pageID - the node to print.
// Output  : None
// Return  : None
// Purpose : Print out the content of the node pid.
//-------------------------------------------------------------------

Status
BTreeFile::PrintNode(PageID pageID)
{
	SortedPage* page = nullptr;
	PIN(pageID, page);

	NodeType type = (NodeType) page->GetType();
	switch (type)
	{
		case INDEX_NODE:
		{
			BTIndexPage* index = (BTIndexPage *) page;
			PageID curPageID = index->GetLeftLink();
			cout << "\n---------------- Content of index node " << pageID << "-----------------------------" << endl;
			cout << "\n Left most PageID:  "  << curPageID << endl;

			RecordID currRid;
			int key, i = 0;

			Status s = index->GetFirst(key, curPageID, currRid);
			while (s != DONE)
			{
				i++;
				cout <<  "Key: " << key << "	PageID: " << curPageID << endl;
				s = index->GetNext(key, curPageID, currRid);
			}
			cout << "\n This page contains  " << i << "  entries." << endl;
			break;
		}

		case LEAF_NODE:
		{
			BTLeafPage* leaf = (BTLeafPage *) page;
			cout << "\n---------------- Content of leaf node " << pageID << "-----------------------------" << endl;

			RecordID dataRid, currRid;
			int key, i = 0;

			Status s = leaf->GetFirst(key, dataRid, currRid);
			while (s != DONE)
			{
				i++;
				cout << "DataRecord ID: " << dataRid << " Key: " << key << endl;
				s = leaf->GetNext(key, dataRid, currRid);
			}
			cout << "\n This page contains  " << i << "  entries." << endl;
			break;
		}
	}
	UNPIN(pageID, CLEAN);

	return OK;
}

//-------------------------------------------------------------------
// BTreeFile::Print
//
// Input   : None
// Output  : None
// Return  : None
// Purpose : Print out this B+ Tree
//-------------------------------------------------------------------

Status
BTreeFile::Print()
{
	cout << "\n\n-------------- Now Begin Printing a new whole B+ Tree -----------" << endl;

	if (PrintTree(rootPid) == OK)
		return OK;

	return FAIL;
}

//-------------------------------------------------------------------
// BTreeFile::DumpStatistics
//
// Input   : None
// Output  : None
// Return  : None
// Purpose : Print out the following statistics.
//           1. Total number of leaf nodes, and index nodes.
//           2. Total number of leaf entries.
//           3. Total number of index entries.
//           4. Mean, Min, and max fill factor of leaf nodes and
//              index nodes.
//           5. Height of the tree.
//-------------------------------------------------------------------
Status
BTreeFile::DumpStatistics()
{
	// TODO: add your code here
	return OK;
}
