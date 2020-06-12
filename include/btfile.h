#ifndef _BTFILE_H
#define _BTFILE_H

#include "btindex.h"
#include "btleaf.h"
#include "index.h"
#include "btfilescan.h"
#include "bt.h"
#include <stack>
#include <string>

class BTreeFile: public IndexFile {

public:

	friend class BTreeFileScan;

	BTreeFile(Status& status, const char* filename);
	~BTreeFile();

	Status DestroyFile();

	Status Insert(const int key, const RecordID rid);
	Status Delete(const int key, const RecordID rid);

	IndexFileScan* OpenScan(const int* lowKey, const int* highKey);

	Status Print();
	Status DumpStatistics();

private:

	// You may add members and methods here.

	PageID rootPid;
    stack<PageID> path;
    char *dbname;

	Status PrintTree(PageID pid);
	Status PrintNode(PageID pid);

    #define LEAF_NODE 1
    #define INDEX_NODE 0
    //sizeof(int) + sizeof(RecordID)
    #define INSERTSIZE 16
    //sizeof(int) + sizeof(PageID)
    #define INDEXENTRYSIZE 12

    void clearPath();
    Status DestroyAll(PageID pageID);
    Status SplitLeafNode(PageID leafPageID, PageID& newRootPageID, const int key, const RecordID rid);
    Status IndexSearch(PageID indexPid, const int key, const RecordID rid, PageID& outPid, string flag);
    Status SplitIndex(PageID prevIndexPageID, const int key, const PageID pid);
    Status LeafInsert(PageID childPid, PageID indexPid, const int key, const RecordID rid);
    Status ReDistributeMerge(PageID childPid);
    PageID getSibilingIndex(PageID childPid, PageID parentPid, bool &right);
    Status IndexReDistributeMerge(PageID indexPid, BTIndexPage *childIndexPage, PageID siblingIndexID, bool right);
    Status MergeIndex(BTIndexPage *childIndexPage, BTIndexPage *siblingPage, BTIndexPage *indexPage, string flag);
    Status MergeLeaf(BTLeafPage *childLeafPage, BTLeafPage *siblingPage, BTIndexPage *indexPage, string flag);
    Status LeafDelete(PageID childPid, PageID indexPid, const int key, const RecordID rid, RecordID& outRid);
    PageID GetLeftLeaf(PageID parentPid);
    PageID GetLastLeaf(PageID parentPid);
};

#endif // _BTFILE_H
