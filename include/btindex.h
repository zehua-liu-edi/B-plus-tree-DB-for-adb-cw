#ifndef BTINDEX_PAGE_H
#define BTINDEX_PAGE_H


#include "minirel.h"
#include "page.h"
#include "sortedpage.h"
#include "bt.h"



class BTIndexPage : public SortedPage {

private:

	// No private or public members should be declared.

public:

	// You may add public methods here.

	Status Insert(const int key, const PageID pid, RecordID& rid);
	Status Delete(const int key, RecordID& rid);

	Status GetFirst(int& key, PageID& pid, RecordID& rid);
	Status GetNext(int& key, PageID& pid, RecordID& rid);
    Status GetLast(int& lastKey, PageID& lastPid, RecordID& rid);
    Status Search (int key, PageID& pid, int& outKey);
    Status leftSearch (int key, PageID& pid, int& outKey);
    Status changeKey (const int newKey, const int targetKey); 

	PageID GetLeftLink(void);
	void SetLeftLink(PageID left);

    PageID GetParentLink(void);
	void SetParentLink(PageID left);

	IndexEntry* GetEntry(int slotNo)
	{
		return (IndexEntry *)(data + slots[slotNo].offset);
	}

	bool IsAtLeastHalfFull()
	{
		return (AvailableSpace() <= (HEAPPAGE_DATA_SIZE) / 2);
	}
};

#endif
