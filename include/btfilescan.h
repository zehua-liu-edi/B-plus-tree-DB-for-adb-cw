#ifndef _BTREE_FILESCAN_H
#define _BTREE_FILESCAN_H

#include "btfile.h"

class BTreeFile;

class BTreeFileScan : public IndexFileScan {

public:

	friend class BTreeFile;

	Status GetNext(RecordID& rid,  int& key);
	Status DeleteCurrent();

	~BTreeFileScan();

private:
    string flag;
	const int *lowKey;
	const int *highKey;
	RecordID scanRid;
	PageID scanPid;
    int currentKey;
    RecordID currentRid;
    BTreeFile* btf;

	void setLowKey(const int *lowkey) {lowKey=lowkey;}
	void setHighKey(const int *highkey) {highKey=highkey;}
	void setPid(PageID pid) {scanPid=pid;}
	void setRid(RecordID rid) {scanRid=rid;}
    void setFlag(string input) {flag=input;}
    void setBtf(BTreeFile* inputBtf) {btf=inputBtf;}
};

#endif
