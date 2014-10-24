#include "rm.h"
#include "TupleItem.h"

#include "AttrCatalogUtility.h"
#include "FileSystemUtility.h"
#include "TableUtility.h"
#include "PageUtility.h"
#include "TupleUtility.h"

#include <math.h>
#include <stdlib.h>
#include <string>
#include <assert.h>
#include <stdio.h>

///////////////////////////////////////////
// Constants
///////////////////////////////////////////

///////////////////////////////////////////
// Helper Function Declarations
///////////////////////////////////////////

///////////////////////////////////////////
// Variables
///////////////////////////////////////////

RM* RM::_rm = 0;
PF_Manager* RM::pf = 0;

///////////////////////////////////////////
// RM Public Class Function Definitions
///////////////////////////////////////////

RM* RM::Instance()
{
	// Attribute Catalog Table name
	
	//TODO: Fix this mess
	const string PRE_CATALOG_ATTRIBUTES_TABLE_NAME = "CS222_Catalog_Attributes";

    if(!_rm)
	{
		//system("rm -f *.dbt");
		//system("rm -f *.ix");
        _rm = new RM();
		if(!_rm->pf)
    		_rm->pf = PF_Manager::Instance();

		// load attribute catalog
		if (doesTableExist(PRE_CATALOG_ATTRIBUTES_TABLE_NAME))
			_rm->loadAttributeCatalog();
	}

    return _rm;
}

RM::RM()
{
}

RM::~RM()
{
}

RC RM::createTable(const string tableName, const vector<Attribute> & attrs)
{
	if (tableName != CATALOG_ATTRIBUTES_TABLE_NAME)
	{
		// create the catalog attributes table if it doesn't already exists
		if (!doesTableExist(CATALOG_ATTRIBUTES_TABLE_NAME))
		{
			// create catalog table
			vector<Attribute> catalogAttrs;
			getAttributeCatalogAttributes(catalogAttrs);

			createTable(CATALOG_ATTRIBUTES_TABLE_NAME, catalogAttrs);
		}
	}

	// create table file
	string tableFilename = getTableFilename(tableName);
	if (pf->CreateFile(tableFilename.c_str()) != 0)
		return -1;

	// open table file
	PF_FileHandle fileHandle;
	if (pf->OpenFile(tableFilename.c_str(), fileHandle) != 0)
		return -1;

	// append directory of pages
	char directoryData[PF_PAGE_SIZE];
	fileHandle.AppendPage(directoryData);
	PageDirectory pDir(fileHandle);
	{
		pDir.ResetData();
		pDir.FlushDataToFile();
		assert(fileHandle.GetNumberOfPages() == 1);
	}

	// close table file
	if (pf->CloseFile(fileHandle) != 0)
		return -1;

	// construct vector to indicate that all attributes (i.e., columns) are valid
	vector<bool> attrsValid;
	for (unsigned i = 0; i < attrs.size(); ++i)
		attrsValid.push_back(true);

	// insert table attributes into cached catalog
	//assert(_catalogAttrTable.find(tableName) == _catalogAttrTable.end());	// make sure key doesn't already exists
	unsigned maxTupleSize = ComputeMaxInternalTupleSize(attrs);		// compute max internal tuple size (to be stored in cached attribute catalog
	TableInfo tInfo = {attrs, attrsValid, maxTupleSize};
	_catalogAttrTable.insert(pair<string, TableInfo >(tableName, tInfo));

	// insert table attributes into catalog file
	RID rid;
	for (unsigned i = 0; i < attrs.size(); ++i)
	{
		TupleItem packedTuple = TupleItem(attrs[i].name) + TupleItem(tableName)
								+ TupleItem(attrs[i].type) + TupleItem(attrs[i].length)
								+ TupleItem(i);

		this->insertTuple(CATALOG_ATTRIBUTES_TABLE_NAME, packedTuple.GetData(), rid);
	}

	return 0;
}

RC RM::deleteTable(const string tableName)
{
	// do not delete catalog, since it's handled internally (and user should not know about it)
	if (CATALOG_ATTRIBUTES_TABLE_NAME == tableName)
		return -1;

	// check that the table exists
	if (!doesTableExist(tableName))
		return -1;

	// remove from catalog cache
	int amtRemoved = _catalogAttrTable.erase(tableName);
	assert(amtRemoved == 1);

	// remove from catalog file
	string tableFileName = getTableFilename(CATALOG_ATTRIBUTES_TABLE_NAME);
	PF_FileHandle fh;
	if (pf->OpenFile(tableFileName.c_str(), fh) == 0)
	{
		// delete all tuples where the tablename attribute value == tablename
		RM_ScanIterator itr;
		vector<string> projectedAttributeNames;
		if (scan(tableName, 
				CATALOG_TABLE_NAME_STRING, 
				EQ_OP, 
				tableName.c_str(),
				projectedAttributeNames, 
				itr))
		{
			RID rid;

			while (itr.getNextTuple(rid, NULL) != RM_EOF)
			{
				deleteTuple(CATALOG_ATTRIBUTES_TABLE_NAME, rid);
			}
		}
	}
	else
		return -1;

	// destroy table file
	pf->DestroyFile(tableFileName.c_str());
	return 0;
}

RC RM::getAttributes(const string tableName, vector<Attribute> & attrs)
{
	assert(!_catalogAttrTable.empty());
	map<string, TableInfo>::iterator itr = _catalogAttrTable.find(tableName);

	if (itr != _catalogAttrTable.end())
	{
		attrs = itr->second.attribute;
		return 0;
	}

	return -1;
}

RC RM::insertTuple(const string tableName, const void *data, RID & rid)
{
	if (!tableExists(tableName))
		return -1;

	PageNum free_page;
	PagePointers ptrs;
	PF_FileHandle fh;
	unsigned recSize = 0;
	unsigned newSlotPos;
	bool reused = false;

	char* rec = (char*)malloc(PF_PAGE_SIZE);
	TableInfo tinf = _catalogAttrTable[tableName];
	char* intRepr = (char*) malloc(GetMaxInternalTupleSize(tinf));
	ExternalToInternalTupleFormat(tinf, data, intRepr, recSize);

	//////////////////////////////////////////////////////////
	// Initialization: Opens file, retrieves directory page, creates PageDirectory object, requests for free space page
	//////////////////////////////////////////////////////////
	string tableFileName = getTableFilename(tableName);
	if (pf->OpenFile(tableFileName.c_str(), fh) == 0)
	{
		PageDirectory pd(fh);

		// query directory for an existing page with sufficient free space
		unsigned requiredSize = recSize + sizeof(SlotStore);
		if (pd.ObtainFreePage(requiredSize, free_page))
		{
			fh.ReadPage(free_page, rec);
			RetrievePagePointers(ptrs, rec);

			assert(*ptrs.size_freespace >= requiredSize);

			// update page directory
			bool hasRemovedPage = pd.RemovePage(free_page, ptrs);
			assert(hasRemovedPage);
		}
		else
		{
			// there isn't any suitable page with free space; allocate new page
			free_page = fh.GetNumberOfPages();
			SetNewPagePointers(ptrs, rec);
			fh.AppendPage(rec);
		}

		// Can we fit in the free space contiguous area?
		if (static_cast<unsigned>(((char*)ptrs.last - (char*)(rec + *ptrs.freespace))) < recSize)
		{
			RearrangePage(ptrs, rec);
			// After rearranging page, we should have enough space (assuming page directory had the information correct
			assert(static_cast<unsigned>(((char*)ptrs.last - (char*)(rec + *ptrs.freespace))) >= recSize);
		}

		// insert tuple data
		memcpy(rec + *ptrs.freespace,intRepr,recSize);

		// Attempt to reuse slots if number of slots grows beyond our statically calculated average
		newSlotPos = *ptrs.slots;
		if (*ptrs.slots > AVGSLOTS)
		{
			SlotStore* it;
			it = ptrs.first;
			for(unsigned i=0; i < *ptrs.slots; i++)
			{
				if (it->slotSize == 0 && it->slotPtr > PF_PAGE_SIZE)
				{
					// reusing previously deleted slot
					newSlotPos = i;
					it->slotSize = recSize;
					it->slotPtr = *ptrs.freespace;
					reused = true;
					break;
				}
				it -= 1;
			}
		}

		// obtain rid
		rid.pageNum = free_page;
		rid.slotNum = newSlotPos;

		// update freespace position, update freespace size, update number of slots and add the new slot data
		if (!reused)
		{
			// determine slot info
			SlotStore newSlot;
			newSlot.slotSize = recSize;
			newSlot.slotPtr = *ptrs.freespace;

			assert(*ptrs.size_freespace >= (recSize + sizeof(SlotStore)));
			*ptrs.size_freespace -= (recSize + sizeof(SlotStore));
			assert(*ptrs.size_freespace < PF_PAGE_SIZE);
			*ptrs.slots += 1;
			*(--ptrs.last) = newSlot;
		}
		*ptrs.freespace += recSize;

		// update page directory (and the page's nextPage#)
		bool isInserted = pd.InsertFreePage(free_page, *ptrs.size_freespace, *ptrs.nextPage);
		assert(isInserted == pd.HasSufficientSpace(*ptrs.size_freespace));
		pd.FlushDataToFile();

		// write page to file
		fh.WritePage(free_page,rec);

		free(intRepr);
		free(rec);

		pf->CloseFile(fh);

		return 0;
	}
	free(intRepr);
	free(rec);
	return -1;
}

RC RM::deleteTuples(const string tableName)
{
	string tableFilename = getTableFilename(tableName);
	PF_FileHandle fh;
	if (pf->OpenFile(tableFilename.c_str(), fh) == 0)
	{
		// reset the directory of page(s)
		PageDirectory pageDir(fh);
		pageDir.ResetData();

		// handle record pages
		unsigned numPages = fh.GetNumberOfPages();
		char pageData[PF_PAGE_SIZE];
		PagePointers pagePtrs;
		for (unsigned i = 1; i < numPages; ++i)
		{
			// read page data
			if (fh.ReadPage(i, pageData) != 0)
			{
				pf->CloseFile(fh);
				return -1;
			}
			
			// reset page data
			SetNewPagePointers(pagePtrs, pageData);

			// insert page into directory of page(s)
			pageDir.InsertFreePage(i, *pagePtrs.size_freespace, *pagePtrs.nextPage);

			// write page data to file
			if (fh.WritePage(i, pageData) != 0)
			{
				pf->CloseFile(fh);
				return -1;
			}
		}

		// flush page directory to file
		pageDir.FlushDataToFile();

		pf->CloseFile(fh);
		return 0;
	}

	return -1;
}

RC RM::deleteTuple(const string tableName, const RID & rid)
{
	PagePointers ptrs;
	PF_FileHandle fh;
	SlotStore* it;
	char* rec = (char*)malloc(PF_PAGE_SIZE);

	string tableFileName = getTableFilename(tableName);
	if (pf->OpenFile(tableFileName.c_str(), fh) == 0)
	{
		if (fh.ReadPage(rid.pageNum, rec) == 0)
		{
			PageDirectory pd(fh);
			{
				// These PageDirectory operations are not atomic and are subject to crashes!
				RetrievePagePointers(ptrs, rec);
				if (pd.RemovePage(rid.pageNum,ptrs))
				{
					// update data
					it = ptrs.first;
					it -= rid.slotNum;
					*ptrs.size_freespace += it->slotSize;
					assert(*ptrs.size_freespace < PF_PAGE_SIZE);
					it->slotSize = 0;
					it->slotPtr = PF_PAGE_SIZE + 1; // Invalid pointer, to differentiate between updatetuple reallocation
					
					// insert pageNum into directory of page(s) using updated data
					if (!pd.InsertFreePage(rid.pageNum, *ptrs.size_freespace, *ptrs.nextPage))
					{
						pf->CloseFile(fh);
					    cout << "****Error: Could not reinsert Page in PageDirectory." << endl;
						free(rec);
						return -1;
					}

					// write data to file
					if (fh.WritePage(rid.pageNum, rec) != 0)
					{
						cout << "****Error: Could not write Page data to file" << endl;
						assert(false);
					}
					free(rec);
				}
				else
				{
					pf->CloseFile(fh);
					cout << "****Error: Could not remove Page from PageDirectory" << endl;
					free(rec);
					return -1;
				}
			}
			pd.FlushDataToFile();
			pf->CloseFile(fh);
			return 0;
		}

		pf->CloseFile(fh);
	}
	free(rec);
	return -1;
}

RC RM::updateTuple(const string tableName, const void *data, const RID & rid)
{
	if (!tableExists(tableName))
		return -1;

	// Modified record may not fit the new page!
	PagePointers ptrs;
	PF_FileHandle fh;
	SlotStore* it;
	char* int_tuple;
	unsigned recSize = 0;

	TableInfo tinf = _catalogAttrTable[tableName];
	int_tuple = (char*) malloc(GetMaxInternalTupleSize(tinf));
	ExternalToInternalTupleFormat(tinf, data, int_tuple, recSize);

	char* rec = (char*)malloc(PF_PAGE_SIZE);
	string tableFileName = getTableFilename(tableName);
	if (pf->OpenFile(tableFileName.c_str(), fh) == 0)
	{
		PageDirectory pd(fh);
		if (fh.ReadPage(rid.pageNum, rec) == 0)
		{
			RetrievePagePointers(ptrs, rec);
			it = ptrs.first;
			it -= rid.slotNum;

			// Is the data here?
			if (it->slotSize == 0)
			{
				if (it->slotPtr > PF_PAGE_SIZE)
				{
					pf->CloseFile(fh);
					free(rec);
					free(int_tuple);
					// Data was deleted! returning error
					return -1;
				}
				else
				{
					// Data was reallocated! Recursively call update to the new RID function
					RID* newrid = (RID*)(rec + it->slotPtr);
					updateTuple(tableName, data, *newrid);
					pf->CloseFile(fh);
					free(rec);
					free(int_tuple);
					return 0;
				}
			}
			else
			{
				if (recSize != it->slotSize)
					pd.RemovePage(rid.pageNum,ptrs);

				// Can we fit the modified tuple on the same spot?
				if (recSize > it->slotSize)
				{
					// If it doesnt fit, we have to delete the tuple
					*ptrs.size_freespace += it->slotSize;
					it->slotSize = 0;

					// Check if we can reinsert in this page's free space. Also remove the current tuple, since we dont need it anymore
					if (*ptrs.size_freespace < recSize)
					{
						// 1. Leave tombstone, delete record
						RID* slot = (RID*)(rec + it->slotPtr);
						RID newlocation;

						// 2. Call insertTuple to reinsert the data on another page
						pd.FlushDataToFile();	// flush data to file so that insertTuple() gets the most updated data.
						{
							insertTuple(tableName,data,newlocation);
						}
						pd.ReloadData();	// reload data that may have been modified by insertTuple().
						slot->pageNum = newlocation.pageNum;
						slot->slotNum = newlocation.slotNum;
						*ptrs.size_freespace -= sizeof(RID);	// Warning: assumes that the previous location has a size >= sizeof(RID). However, in our case, this is always true.
						assert(*ptrs.size_freespace < PF_PAGE_SIZE);
					}
					else
					{
						// Have enough space in the page, but I might require rearranging
						if ((unsigned)((char*)ptrs.last - (char*)(rec + *ptrs.freespace)) < recSize)
						{
							// indicate that the slot is deleted
							it->slotPtr = PF_PAGE_SIZE + 1;	
							assert(IsSlotFree(it));

							RearrangePage(ptrs,rec);

							// need to reupdate var it, since RearrangePage() deletes the old buffer and reallocates a new one
							it = ptrs.first;
							it -= rid.slotNum;
						}
						memcpy(rec + *ptrs.freespace, int_tuple, recSize);
					
						it->slotSize = recSize;
					    it->slotPtr = *ptrs.freespace;

						*ptrs.freespace += recSize;
						*ptrs.size_freespace -= recSize;
						assert(*ptrs.size_freespace < PF_PAGE_SIZE);
					}

				}
				// The new tuple is smaller, simple case.
				else
				{
					memcpy(rec + it->slotPtr,int_tuple, recSize);
					assert(it->slotSize >= recSize);
					*ptrs.size_freespace += (it->slotSize - recSize);
					assert(*ptrs.size_freespace < PF_PAGE_SIZE);
					it->slotSize = recSize;
				}
				pd.InsertFreePage(rid.pageNum, *ptrs.size_freespace, *ptrs.nextPage);
				pd.FlushDataToFile();
				fh.WritePage(rid.pageNum, rec);
				free(rec);
				free(int_tuple);
				pf->CloseFile(fh);
				return 0;
			}
		}
		pf->CloseFile(fh);
	}
	free(rec);
	free(int_tuple);
	return -1;
}

RC RM::readTuple(const string tableName, const RID & rid, void *data)
{
	if (!tableExists(tableName))
		return -1;

	PagePointers ptrs;
	PF_FileHandle fh;
	SlotStore* it;
	unsigned recSize = 0;

	TableInfo tinf = _catalogAttrTable[tableName];
	char* rec = (char*)malloc(PF_PAGE_SIZE);
	string tableFileName = getTableFilename(tableName);
	if (pf->OpenFile(tableFileName.c_str(), fh) == 0)
	{
		if (fh.ReadPage(rid.pageNum, rec) == 0)
		{
			RetrievePagePointers(ptrs, rec);

			// check whether the slotnum is out-of-range
			if (rid.slotNum >= *ptrs.slots)
			{
				free(rec);
				pf->CloseFile(fh);
				return -1;
			}
			else
			{
				it = ptrs.first;
				it -= rid.slotNum;
				if (it->slotSize > 0)
				{
					InternalToExternalTupleFormat(tinf, rec + it->slotPtr, data, recSize);
					free(rec);
					pf->CloseFile(fh);
					return 0;
				}
				// slotSize = 0 -> slot deleted, slotPtr is valid ( < PF_PAGE_SIZE) -> it was reallocated
				else if(it->slotSize == 0 && it->slotPtr < PF_PAGE_SIZE)
				{
					RID* newrid = (RID*)(rec + it->slotPtr);
					if (readTuple(tableName, *newrid, data) == 0)
					{
						free(rec);
						pf->CloseFile(fh);
						return 0;
					}
					else
						cout << "Failed to read tuple." << endl;
					// else: Failed to read tuple
				}
				// else: Data was deleted, not reallocated. Free resources, return -1
			}
		}

		pf->CloseFile(fh);
	}
	free(rec);
	return -1;
}

RC RM::readAttribute(const string tableName, const RID & rid, const string attributeName, void *data)
{
	if (!tableExists(tableName))
		return -1;

	PagePointers ptrs;
	PF_FileHandle fh;
	SlotStore* ss;
	vector<Attribute>::iterator it;

	// retrieve tuple data
	TableInfo tinf = _catalogAttrTable[tableName];
	char* rec = (char*)malloc(PF_PAGE_SIZE);
	char* slot;
	string tableFileName = getTableFilename(tableName);
	if (pf->OpenFile(tableFileName.c_str(), fh) == 0)
	{
		if (fh.ReadPage(rid.pageNum, rec) == 0)
		{
			RetrievePagePointers(ptrs, rec);

			// check if slot# is in range
			if (rid.slotNum >= *ptrs.slots)
			{
				free(rec);
				pf->CloseFile(fh);
				return -1;
			}

			// obtain slot data
			ss = ptrs.first;
			ss -= rid.slotNum;
			slot = rec + ss->slotPtr;

			// check that the slot is valid (i.e., not deleted or moved)
			if (ss->slotSize == 0)
			{
				// check if deleted
				if (ss->slotPtr >= PF_PAGE_SIZE)
				{
					free(rec);
					pf->CloseFile(fh);
					return -1;
				}
				else
				{
					// data has been moved to another page
					RID* newRID = reinterpret_cast<RID*>(slot);
					if (readTuple(tableName, *newRID, rec) != 0)
					{
						free(rec);
						pf->CloseFile(fh);
						return -1;
					}

					// re-obtain data
					RetrievePagePointers(ptrs, rec);
					ss = ptrs.first;
					ss -= rid.slotNum;
					slot = rec + ss->slotPtr;
				}
			}
		}
		else
		{
			free(rec);
			pf->CloseFile(fh);
			return -1;
		}
		pf->CloseFile(fh);
	}

	// retrieve attribute data
	unsigned attrIndex = 0;
	unsigned dataSize;
	for ( it=tinf.attribute.begin(); it < tinf.attribute.end(); it++, ++attrIndex )
	{
		if (it->name == attributeName)
		{
			// check that the attribute is valid (i.e., not deleted)
			if (!tinf.attrValidity[attrIndex])
				break;

			GetTupleAttribute(tinf, slot, attrIndex, data, dataSize);
			free(rec);
			return 0;
		}
	}

	free(rec);
	return -1;
}

RC RM::reorganizePage(const string tableName, const unsigned  pageNumber)
{
	PagePointers ptrs;
	PF_FileHandle fh;

	char* rec = (char*)malloc(PF_PAGE_SIZE);
	string tableFileName = getTableFilename(tableName);
	if (pf->OpenFile(tableFileName.c_str(), fh) == 0)
	{
		if (fh.ReadPage(pageNumber, rec) == 0)
		{
			RetrievePagePointers(ptrs, rec);
			RearrangePage(ptrs,rec);

			// write updated data to file
			if (fh.WritePage(pageNumber, rec) != 0)
			{
				free(rec);
				pf->CloseFile(fh);
				return -1;
			}

			free(rec);
			pf->CloseFile(fh);
			return 0;
		}

		pf->CloseFile(fh);
	}
	free(rec);
	return -1;
}

RC RM::scan(const string tableName, const string conditionAttribute, const CompOp compOp, const void *value, const vector<string> & attributeNames, RM_ScanIterator & rm_ScanIterator)
{
	rm_ScanIterator = RM_ScanIterator();

	// retrieve tableInfo
	if (!getTableInfo(tableName, rm_ScanIterator._tableInfo))
		return -1;

	// open file
	assert(doesTableExist(tableName));
	string tableFileName = getTableFilename(tableName);
	rm_ScanIterator._pFileHandle = new PF_FileHandle();
	if (pf->OpenFile(tableFileName.c_str(), *rm_ScanIterator._pFileHandle) == 0)
	{
		// start at page 1 (i.e., first data page)
		rm_ScanIterator._currPageNum = 1;

		// retrieve page data and page related data
		PF_FileHandle& fh = *rm_ScanIterator._pFileHandle;
		if (rm_ScanIterator._currPageNum >= fh.GetNumberOfPages())
		{
			rm_ScanIterator._slotPtr = NULL;	// indicate that there is no data pages
			return 0;
		}
		rm_ScanIterator._pFileHandle->ReadPage(rm_ScanIterator._currPageNum, rm_ScanIterator._pageData);
		RetrievePagePointers(rm_ScanIterator._pagePtrs, rm_ScanIterator._pageData);
		rm_ScanIterator._slotPtr = rm_ScanIterator._pagePtrs.first;

		// retrieve projected attr positions
		unsigned numProjectedAttrs = attributeNames.size();
		unsigned attrPos;
		for (unsigned i = 0; i < numProjectedAttrs; ++i)
		{
			if (!GetAttributePosition(rm_ScanIterator._tableInfo, attributeNames[i], attrPos))
			{
				rm_ScanIterator = RM_ScanIterator();
				return -1;
			}

			rm_ScanIterator._attrPositions.push_back(attrPos);
		}

		// set comparision operation
		rm_ScanIterator._compOp = compOp;

		// since some parameters can be empty or NULL, we need to do checking
		if (compOp != NO_OP)
		{
			// get attribute type
			Attribute attr;
			if (!GetAttributeDetail(rm_ScanIterator._tableInfo, conditionAttribute, attr))
			{
				rm_ScanIterator = RM_ScanIterator();
				return -1;
			}
			rm_ScanIterator._attrType = attr.type;

			// get attribute position
			if (!GetAttributePosition(rm_ScanIterator._tableInfo, conditionAttribute, rm_ScanIterator._compAttrPosition))
			{
				rm_ScanIterator = RM_ScanIterator();
				return -1;
			}

			// set comparison value
			rm_ScanIterator._compValue = value;
		}	
	}
	else
		return -1;

	return 0;
}

RC RM::dropAttribute(const string tableName, const string attributeName)
{
	return -1;
}

RC RM::addAttribute(const string tableName, const Attribute attr)
{
	return -1;
}

RC RM::reorganizeTable(const string tableName)
{
	return -1;
}

///////////////////////////////////////////
// RM Protected/Private Class Function Definitions
///////////////////////////////////////////

bool RM::loadAttributeCatalog()
{
	assert(_catalogAttrTable.empty());

	// construct temporary table information
	vector<Attribute> catalogAttrs;
	getAttributeCatalogAttributes(catalogAttrs);
	unsigned maxTupleSize = ComputeMaxInternalTupleSize(catalogAttrs);		// compute max internal tuple size (to be stored in cached attribute catalog
	
	vector<bool> attrsValid;
	for (unsigned i = 0; i < catalogAttrs.size(); ++i)
		attrsValid.push_back(true);

	TableInfo tableInfo = {catalogAttrs, attrsValid, maxTupleSize};

	// obtain scan iterator
	RM_ScanIterator itr;
	if (!getSequentialScanIterator(tableInfo, CATALOG_ATTRIBUTES_TABLE_NAME, itr))
		return false;

	// scan all tuples into attribute catalog cache
	string attrName, tableName;
	int attrType, attrLength, attrPosition;
	map<string, vector<unsigned> > attrPositionsMap;
	unsigned numAttributes = catalogAttrs.size();
	RID rid;
	char* data = new char[tableInfo.maxInternalTupleSize];
	unsigned dataOffset;
	while (itr.getNextTuple(rid, data) != RM_EOF)
	{
		// initialize data
		attrName = tableName = "";
		attrType = attrLength = attrPosition = -1;
		dataOffset = 0;

		// retrieve tuple data
		for (unsigned i = 0; i < numAttributes; ++i)
		{
			if (catalogAttrs[i].name == CATALOG_ATTR_NAME_STRING)
			{
				attrName = ExtractString(data + dataOffset);
				dataOffset += attrName.length() + TYPE_INT_SIZE;
			}
			else if (catalogAttrs[i].name == CATALOG_ATTR_TYPE_STRING)
			{
				attrType = ExtractInt(data + dataOffset);
				dataOffset += TYPE_INT_SIZE;
			}
			else if (catalogAttrs[i].name == CATALOG_ATTR_LENGTH_STRING)
			{
				attrLength = ExtractInt(data + dataOffset);
				dataOffset += TYPE_INT_SIZE;
			}
			else if (catalogAttrs[i].name == CATALOG_POSITION_STRING)
			{
				attrPosition = ExtractInt(data + dataOffset);
				dataOffset += TYPE_INT_SIZE;
			}
			else if (catalogAttrs[i].name == CATALOG_TABLE_NAME_STRING)
			{
				tableName = ExtractString(data + dataOffset);
				dataOffset += tableName.length() + TYPE_INT_SIZE;
			}
		}

		// insert tuple into cached catalog
		Attribute attr(attrName, static_cast<AttrType>(attrType), attrLength);
		TableInfo& tInfo = _catalogAttrTable[tableName];
		if (tInfo.attribute.empty())
		{
			tInfo.attribute.push_back(attr);
			attrPositionsMap[tableName].push_back(attrPosition);
		}
		else
		{
			// put attribute tuple into appropriate order (in the vector) based on its position
			vector<Attribute>::reverse_iterator attrItr = tInfo.attribute.rend();
			vector<unsigned>& positions = attrPositionsMap[tableName];
			vector<unsigned>::reverse_iterator posItr = positions.rend();
			while (posItr < positions.rend())
			{
				if (static_cast<unsigned>(attrPosition) > *posItr)
				{
					tInfo.attribute.insert((attrItr+1).base(), attr);
					positions.insert((posItr+1).base(), attrPosition);
				}

				// update iterators
				++attrItr;
				++posItr;
			}
		}
		tInfo.attrValidity.push_back(true);
	}
	delete [] data;

	// compute necessary TableInfo data
	map<string, TableInfo>::iterator mapItr = _catalogAttrTable.begin();
	while(mapItr != _catalogAttrTable.end())
	{
		mapItr->second.maxInternalTupleSize = ComputeMaxInternalTupleSize(mapItr->second.attribute);
		++mapItr;
	}

	return true;
}

void RM::getAttributeCatalogAttributes(vector<Attribute>& attrs)
{
	attrs.clear();

	Attribute attr_name(CATALOG_ATTR_NAME_STRING, TypeVarChar, MAX_ATTR_CATALOG_STRING_COL_LENGTH);
	Attribute table_name(CATALOG_TABLE_NAME_STRING, TypeVarChar, MAX_ATTR_CATALOG_STRING_COL_LENGTH);
	Attribute type(CATALOG_ATTR_TYPE_STRING, TypeInt, sizeof(int));
	Attribute length(CATALOG_ATTR_LENGTH_STRING, TypeInt, sizeof(int));
	Attribute position(CATALOG_POSITION_STRING, TypeInt, sizeof(int));

	attrs.push_back(attr_name);
	attrs.push_back(table_name);
	attrs.push_back(type);
	attrs.push_back(length);
	attrs.push_back(position);
}

bool RM::tableExists(const string& tableName) const
{
	map<string, TableInfo >::const_iterator itr = _catalogAttrTable.find(tableName);

	if (itr == _catalogAttrTable.end())
		return false;

	return true;
}

bool RM::getTableInfo(const string& tableName, TableInfo& tableInfo)
{
	map<string, TableInfo >::iterator itr = _catalogAttrTable.find(tableName);

	if (itr == _catalogAttrTable.end())
		return false;

	tableInfo = itr->second;
	return true;
}

bool RM::getSequentialScanIterator(const TableInfo& tableInfo, const string& tableName, RM_ScanIterator& rm_ScanIterator) const
{
	rm_ScanIterator = RM_ScanIterator();

	// set tableInfo
	rm_ScanIterator._tableInfo = tableInfo;

	// open file
	string tableFileName = getTableFilename(tableName);
	rm_ScanIterator._pFileHandle = new PF_FileHandle();
	if (pf->OpenFile(tableFileName.c_str(), *rm_ScanIterator._pFileHandle) == 0)
	{
		// start at page 1 (i.e., first data page)
		rm_ScanIterator._currPageNum = 1;

		// retrieve page data and page related data
		PF_FileHandle& fh = *rm_ScanIterator._pFileHandle;
		if (rm_ScanIterator._currPageNum >= fh.GetNumberOfPages())
		{
			rm_ScanIterator._slotPtr = NULL;	// indicate that there is no data pages
			return true;
		}
		rm_ScanIterator._pFileHandle->ReadPage(rm_ScanIterator._currPageNum, rm_ScanIterator._pageData);
		RetrievePagePointers(rm_ScanIterator._pagePtrs, rm_ScanIterator._pageData);
		rm_ScanIterator._slotPtr = rm_ScanIterator._pagePtrs.first;

		// retrieve projected attr positions
		GetAllAttributePositions(tableInfo, rm_ScanIterator._attrPositions);

		// set comparison operation to no operation so that we can scan all records
		rm_ScanIterator._compOp = NO_OP;
	}
	else
		return false;

	return true;
}

///////////////////////////////////////////
// RM_ScanIterator Class Function Definitions
///////////////////////////////////////////

RC RM_ScanIterator::close() 
{ 
	if (_pFileHandle == NULL)
		return 0;

	PF_Manager* pf = PF_Manager::Instance();
	RC result = pf->CloseFile(*_pFileHandle);

	delete _pFileHandle;
	_pFileHandle = NULL;

	return result;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void* data)
{ 
	if (_pFileHandle == NULL)
		return -1;

	// special case: when data pages don't exist
	if (_slotPtr == NULL)
		return RM_EOF;

	char* tempData = new char[_tableInfo.maxInternalTupleSize];
	RC returnVal;
	while (true)
	{
		// check whether we're done with this page
		if (_slotPtr < _pagePtrs.last)
		{
			// go to next page
			assert(_currPageNum <= _pFileHandle->GetNumberOfPages());
			++_currPageNum;

			if (_currPageNum < _pFileHandle->GetNumberOfPages())
			{
				_pFileHandle->ReadPage(_currPageNum, _pageData);
				RetrievePagePointers(_pagePtrs, _pageData);
				_slotPtr = _pagePtrs.first;
			}
			else
			{
				returnVal = RM_EOF;
				break;
			}
		}

		// check if slot is valid
		if (_slotPtr->slotSize <= 0)
		{
			--_slotPtr;
			continue;
		}

		// check if comparison operation is required
		if (_compOp != NO_OP)
		{
			// get attribute data
			unsigned dataSize;
			GetTupleAttribute(_tableInfo, _pageData + _slotPtr->slotPtr, _compAttrPosition, tempData, dataSize);

			// do comparison
			if (_attrType == TypeVarChar)
			{
				string attrData(tempData + TYPE_VARCHAR_SIZE, dataSize - TYPE_VARCHAR_SIZE);
				const char* compValuePtr = reinterpret_cast<const char*>(_compValue);
				unsigned compValueLength = *reinterpret_cast<const unsigned*>(compValuePtr);
				string compValue(compValuePtr + TYPE_VARCHAR_SIZE, compValueLength);

				// check comparison
				if (!Compare(attrData, compValue))
				{
					// update slot pointer
					--_slotPtr;
					continue;	
				}
			}
			else if (_attrType == TypeInt)
			{
				int attrData = *reinterpret_cast<int*>(tempData);
				int compValue = *reinterpret_cast<const int*>(_compValue);

				// check comparison
				if (!Compare(attrData, compValue))
				{
					// update slot pointer
					--_slotPtr;
					continue;	
				}
			}
			else if (_attrType == TypeReal)
			{
				float attrData = *reinterpret_cast<float*>(tempData);
				float compValue = *reinterpret_cast<const float*>(_compValue);

				// check comparison
				if (!Compare(attrData, compValue))
				{
					// update slot pointer
					--_slotPtr;
					continue;	
				}
			}
					  
		}

		// output data
		unsigned numAttrs = _attrPositions.size();

		unsigned attrDataSize;
		unsigned dataOffset = 0;
		char* dataPtr = reinterpret_cast<char*>(data);
		for (unsigned i = 0; i < numAttrs; ++i)
		{
			// output attribute data
			GetTupleAttribute(_tableInfo, _pageData + _slotPtr->slotPtr, _attrPositions[i], dataPtr + dataOffset, attrDataSize);

			// update offset
			dataOffset += attrDataSize;
		}

		returnVal = 0;
		rid.pageNum = _currPageNum;
		rid.slotNum = (reinterpret_cast<char*>(_pagePtrs.first) - reinterpret_cast<char*>(_slotPtr)) / sizeof(SlotStore);
		--_slotPtr;	// update slot
		break;
	}

	delete [] tempData;

	return returnVal; 
}

bool RM_ScanIterator::Compare(const string& attrData, const string& compValue) const
{
	switch (_compOp)
	{
	case EQ_OP:
		{
			if (attrData == compValue)
				return true;
		}
		break;
	case LT_OP:
		{
			if (attrData < compValue)
				return true;
		}
		break;
	case GT_OP:
		{
			if (attrData > compValue)
				return true;
		}
		break;
	case LE_OP:
		{
			if (attrData <= compValue)
				return true;
		}
		break;
	case GE_OP:
		{
			if (attrData >= compValue)
				return true;
		}
		break;
	case NE_OP:
		{
			if (attrData != compValue)
				return true;
		}
		break;
	default:
		assert(false);
	};

	return false;
}

bool RM_ScanIterator::Compare(const int attrData, const int compValue) const
{
	switch (_compOp)
	{
	case EQ_OP:
		{
			if (attrData == compValue)
				return true;
		}
		break;
	case LT_OP:
		{
			if (attrData < compValue)
				return true;
		}
		break;
	case GT_OP:
		{
			if (attrData > compValue)
				return true;
		}
		break;
	case LE_OP:
		{
			if (attrData <= compValue)
				return true;
		}
		break;
	case GE_OP:
		{
			if (attrData >= compValue)
				return true;
		}
		break;
	case NE_OP:
		{
			if (attrData != compValue)
				return true;
		}
		break;
	default:
		assert(false);
	};

	return false;
}

bool RM_ScanIterator::Compare(const float attrData, const float compValue) const
{
	switch (_compOp)
	{
	case EQ_OP:
		{
			if (attrData == compValue)
				return true;
		}
		break;
	case LT_OP:
		{
			if (attrData < compValue)
				return true;
		}
		break;
	case GT_OP:
		{
			if (attrData > compValue)
				return true;
		}
		break;
	case LE_OP:
		{
			if (attrData <= compValue)
				return true;
		}
		break;
	case GE_OP:
		{
			if (attrData >= compValue)
				return true;
		}
		break;
	case NE_OP:
		{
			if (attrData != compValue)
				return true;
		}
		break;
	default:
		assert(false);
	};

	return false;
}

///////////////////////////////////////////
// Helper Function Definitions
///////////////////////////////////////////

