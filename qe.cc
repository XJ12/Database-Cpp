
#include "qe.h"

#include "../rm/TupleUtility.h"

enum ERROR_CODE{ NOT_ENOUGH_MEMORY = 1,
				 DUPLICATE_KEY_OR_NOT_ENOUGH_MEMORY,
				 UNKNOWN_ERROR};


///////////////////////////////////////////////
// Project Function Definitions
///////////////////////////////////////////////

Project::Project(Iterator *input,
				 const vector<string> &attrNames)
{
	_itr = input;

	if (_itr != NULL)
	{
		// retrieve itr attributes
		_itr->getAttributes(_attrs);
		
		// find matching names
		unsigned numFoundMatches = 0;
		unsigned maxItrDataSize = 0;
		bool isNameFound;
		for (unsigned i = 0; i < _attrs.size(); ++i)
		{
			maxItrDataSize += _attrs[i].length;
			isNameFound = false;
			for (unsigned n = 0; n < attrNames.size(); ++n)
			{
				if (_attrs[i].name == attrNames[n])
				{
					_projectedAttrs.push_back(_attrs[i]);
					isNameFound = true;
					++numFoundMatches;
					break;
				}
			}

			_isProjectedAttr.push_back(isNameFound);
		}

		if (numFoundMatches == attrNames.size())
		{
			// allocate buffer
			_buffer = new char[maxItrDataSize];
			return;
		}
	}

	// _itr == NULL or appropriate # of attrNames were not found
	_buffer = NULL;
}

Project::~Project()
{
	if (_buffer != NULL)
		delete [] _buffer;
}

RC Project::getNextTuple(void* data)
{
	if (_itr != NULL && _buffer != NULL)
	{
		RC result;

		// get tuple data
		result = _itr->getNextTuple(_buffer);

		if (result == 0)
		{
			unsigned bufferItr = 0;
			unsigned dataItr = 0;

			// for each attribute data
			assert(_isProjectedAttr.size() == _attrs.size());
			unsigned dataLength;
			for (unsigned i = 0; i < _isProjectedAttr.size(); ++i)
			{
				// compute data length
				if (_attrs[i].type == TypeVarChar)
				{
					unsigned stringLength;
					memcpy(&stringLength, _buffer + dataItr, sizeof(unsigned));
					dataLength = stringLength + sizeof(unsigned);
				}
				else
				{
					dataLength = _attrs[i].length;
					assert(dataLength == sizeof(int));
				}

				// copy over attribute data
				if (_isProjectedAttr[i])
				{
					memcpy(reinterpret_cast<char*>(data) + dataItr, _buffer + bufferItr, dataLength);
					
					// update iterators
					bufferItr += dataLength;
					dataItr += dataLength;
				}
				else
				{
					// update buffer iterator
					bufferItr += dataLength;
				}
			}

			return 0;
		}
		else
			return result;
	}

	return -1;
}

void Project::getAttributes(vector<Attribute> &attrs) const
{
	attrs = _projectedAttrs;
}

////////////////////////////////////////////////////////////////
// Filter Function Definitions
////////////////////////////////////////////////////////////////

Filter::Filter(Iterator *input, const Condition & condition)
{
	input->getAttributes(curAttrs);
	original = input;
	cond = condition;
}


Filter::~Filter()
{
}

RC Filter::AttrPosition(string name, char* data, unsigned& bufferpos, unsigned& pos, unsigned& nchars)
{
	bufferpos = 0;
	pos = 0;
	for(unsigned i = 0; i < curAttrs.size(); ++i)
	{
		if (curAttrs[i].type == TypeVarChar)
			nchars = *(unsigned*)((char*)(data+bufferpos));

		if (name.compare(curAttrs[i].name) == 0)
		{
			pos = i;
			return 0;
		}
		else
			bufferpos += curAttrs[i].length;
	}
	bufferpos = -1;
	pos = -1;
	return -1;
}

RC Filter::getNextTuple(void *data)
{
	RC ret;
	bool comp = false;
	char* tuple = (char*)data;
	unsigned loff, roff;
	unsigned lpos, rpos;
	unsigned lchars = 0, rchars = 0;

	while(comp == false)
	{
		if ((ret = original->getNextTuple((void*)tuple)) != 0)
			return ret;
		ret = AttrPosition(cond.lhsAttr, tuple, loff, lpos, lchars);
		assert(ret == 0);
		if(cond.bRhsIsAttr)
		{
			ret = AttrPosition(cond.rhsAttr, tuple, roff, rpos, rchars);
			assert(ret == 0);
		}
		else
			roff = 0;
		// TODO: Converting right hand side to the same type of left hand side. Is that ok?
		if (curAttrs[lpos].type == TypeInt)
			comp = compareValues<int>(*(int*)((char*)(tuple + loff)), *(int*)((char*)(tuple + roff)));
		else if (curAttrs[lpos].type == TypeReal)
			comp = compareValues<float>(*(float*)((char*)(tuple + loff)), *(float*)((char*)(tuple + roff)));
		else
		{
			if (lchars == rchars)
				comp = compareStrings((char*)(tuple + loff),(char*)(tuple + roff), lchars);
			else
				comp = false;
		}
	}
	return 0;
}

template<typename KeyType>
bool Filter::compareValues(KeyType lvalue, KeyType rvalue)
{
	if (!cond.bRhsIsAttr)
		rvalue = *(KeyType*) cond.rhsValue.data;
	switch(cond.op)
	{
	case GE_OP:
		if (lvalue >= rvalue)
			return true;
		break;
	case GT_OP:
		if (lvalue > rvalue)
			return true;
		break;
	case LT_OP:
		if (lvalue < rvalue)
			return true;
		break;
	case LE_OP:
		if (lvalue <= rvalue)
			return true;
		break;
	case EQ_OP:
		if (lvalue == rvalue)
			return true;
		break;
	case NE_OP:
		if (lvalue != rvalue)
			return true;
		break;
	default:
		cout << "Error: Operation not supported" << endl;
		return false;
	}
	return false;
}

bool Filter::compareStrings(char* lvalue, char* rvalue, unsigned nchars)
{
	char* tuple;
	if (!cond.bRhsIsAttr)
	{
		tuple = (char*)cond.rhsValue.data;
		nchars = *(unsigned*)(cond.rhsValue.data);
		rvalue = (tuple+4);
	}
	switch(cond.op)
	{
	case EQ_OP:
		if (strncmp (lvalue,rvalue, nchars) == 0)
			return true;
		break;
	case NE_OP:
		if (strncmp (lvalue,rvalue, nchars) == 0)
			return false;
		else
			return true;
	default:
		cout << "Error: Operation not supported" << endl;
		return false;

	}
	return false;
}


void Filter::getAttributes(vector<Attribute> & attrs) const
{
	attrs = curAttrs;
}


RC NLJoin::AttrPosition(string name, vector<Attribute> curAttrs, char* data, unsigned& bufferpos, unsigned& pos, unsigned& nchars)
{
	bufferpos = 0;
	pos = 0;
	nchars = 0;
	for(unsigned i = 0; i < curAttrs.size(); ++i)
	{
		if (curAttrs[i].type == TypeVarChar)
			nchars = *(unsigned*)((char*)(data+bufferpos));

		if (name.compare(curAttrs[i].name) == 0)
		{
			pos = i;
			return 0;
		}
		else
		{
			if (curAttrs[i].type == TypeInt || curAttrs[i].type == TypeReal)
				bufferpos += 4;
			else
				bufferpos += 4 + nchars;
		}
	}
	bufferpos = -1;
	pos = -1;
	return -1;
}


NLJoin::NLJoin(Iterator *leftIn, TableScan *rightIn, const Condition & condition, const unsigned  numPages)
{
	leftJoin = leftIn;
	rightJoin = rightIn;
	cond = condition;
	nump = numPages;
	unsigned bufferpos, pos, nchars;

	leftJoin->getAttributes(leftAttrs);
	rightJoin->getAttributes(rightAttrs);
	lsize = 0;
	rsize = 0;
	for(unsigned i = 0; i < leftAttrs.size(); ++i)
		lsize += leftAttrs[i].length;
	for(unsigned i = 0; i < rightAttrs.size(); ++i)
		rsize += rightAttrs[i].length;

	left = (char*)malloc(lsize);
	right = (char*)malloc(rsize);

	RC retVal = leftJoin->getNextTuple((void*)left);
	if (retVal != 0)
	{
		free(left);
		left = NULL;	// indicate that there are no tuples
		return;
	}

	innerCond.lhsAttr = cond.rhsAttr;
	innerCond.op = cond.op;
	innerCond.bRhsIsAttr = false;
	retVal = AttrPosition(cond.lhsAttr, leftAttrs, left, bufferpos, pos, nchars);
	assert(retVal == 0);
	innerCond.rhsValue.type = leftAttrs[pos].type;
	innerCond.rhsValue.data = (void*)(left + bufferpos);
	inner = new Filter(rightJoin, innerCond);
}



NLJoin::~NLJoin()
{
	if (left != NULL)
		free(left);
	if (right != NULL)
		free(right);
	if (inner != NULL)
		delete inner;
}

RC NLJoin::getNextTuple(void* data)
{
	Condition newcond;
	RC ret;
	unsigned bufferpos, pos, nchars;

	if (left == NULL)
		return QE_EOF;

	while (inner->getNextTuple((void*)right) == QE_EOF)
	{
		if (leftJoin->getNextTuple((void*)left) == QE_EOF)
			return QE_EOF;
		delete inner;
		rightJoin->setIterator();
		innerCond.lhsAttr = cond.rhsAttr;
		innerCond.op = cond.op;
		if ((ret = AttrPosition(cond.lhsAttr, leftAttrs, left, bufferpos, pos, nchars)) != 0)
			return ret;
		innerCond.rhsValue.type = leftAttrs[pos].type;
		innerCond.rhsValue.data = (void*)(left + bufferpos);
		inner = new Filter(rightJoin, innerCond);
	}
	unsigned leftTupleSize, rightTupleSize;
	GetExternalDataSize(left, leftAttrs, leftTupleSize);
	GetExternalDataSize(right, rightAttrs, rightTupleSize);
	memcpy(data,left,leftTupleSize);
	memcpy(reinterpret_cast<char*>(data) + lsize, right, rightTupleSize);
	//TODO: how to free memory? Save last tuple and free it on nextTuple?
	return 0;
}

void NLJoin::getAttributes(vector<Attribute> & attrs) const
{
	attrs.insert(attrs.begin(), leftAttrs.begin(), leftAttrs.end());
	attrs.insert(attrs.end(), rightAttrs.begin(), rightAttrs.end());
}



INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition & condition, const unsigned  numPages)
{
	leftJoin = leftIn;
	rightJoin = rightIn;
	cond = condition;
	nump = numPages;
	unsigned bufferpos, pos, nchars;

	leftJoin->getAttributes(leftAttrs);
	rightJoin->getAttributes(rightAttrs);

	lsize = 0;
	rsize = 0;
	for(unsigned i = 0; i < leftAttrs.size(); ++i)
		lsize += leftAttrs[i].length;
	for(unsigned i = 0; i < rightAttrs.size(); ++i)
		rsize += rightAttrs[i].length;

	left = (char*)malloc(lsize);
	right = (char*)malloc(rsize);

	RC retVal = leftJoin->getNextTuple((void*)left);
	if (retVal != 0)
	{
		free(left);
		left = NULL;	// indicate that there are no tuples
		return;
	}


	innerCond.lhsAttr = cond.rhsAttr;
	innerCond.op = cond.op;
	innerCond.bRhsIsAttr = false;
	retVal = AttrPosition(cond.lhsAttr, leftAttrs, left, bufferpos, pos, nchars);
	assert(retVal == 0);
	innerCond.rhsValue.type = leftAttrs[pos].type;
	innerCond.rhsValue.data = (void*)(left + bufferpos);
	rightJoin->setIterator(cond.op, innerCond.rhsValue.data);
	inner = rightJoin;
}

INLJoin::~INLJoin()
{
	if (left != NULL)
		free(left);
	if (right != NULL)
		free(right);
}

RC INLJoin::getNextTuple(void* data)
{
	Condition newcond;
	RC ret;
	unsigned bufferpos, pos, nchars;

	// check if the outer (i.e., left) relation is empty
	if (left == NULL)
		return QE_EOF;

	while (inner->getNextTuple((void*)right) == QE_EOF)
	{
		if (leftJoin->getNextTuple((void*)left) == QE_EOF)
			return QE_EOF;
		innerCond.lhsAttr = cond.rhsAttr;
		innerCond.op = cond.op;
		if ((ret = AttrPosition(cond.lhsAttr, leftAttrs, left, bufferpos, pos, nchars)) != 0)
			return ret;
		innerCond.rhsValue.type = leftAttrs[pos].type;
		innerCond.rhsValue.data = (void*)(left + bufferpos);
		inner->setIterator(cond.op, innerCond.rhsValue.data);
	}

	unsigned leftTupleSize, rightTupleSize;
	GetExternalDataSize(left, leftAttrs, leftTupleSize);
	GetExternalDataSize(right, rightAttrs, rightTupleSize);
	memcpy(data,left,leftTupleSize);
	memcpy(reinterpret_cast<char*>(data) + lsize, right, rightTupleSize);
	//TODO: how to free memory? Save last tuple and free it on nextTuple?
	return 0;
}

void INLJoin::getAttributes(vector<Attribute> & attrs) const
{
	attrs.insert(attrs.begin(), leftAttrs.begin(), leftAttrs.end());
	attrs.insert(attrs.end(), rightAttrs.begin(), rightAttrs.end());
}

RC INLJoin::AttrPosition(string name, vector<Attribute> curAttrs, char* data, unsigned& bufferpos, unsigned& pos, unsigned& nchars)
{
	bufferpos = 0;
	pos = 0;
	nchars = 0;
	for(unsigned i = 0; i < curAttrs.size(); ++i)
	{
		if (curAttrs[i].type == TypeVarChar)
			nchars = *(unsigned*)((char*)(data+bufferpos));

		if (name.compare(curAttrs[i].name) == 0)
		{
			pos = i;
			return 0;
		}
		else
		{
			if (curAttrs[i].type == TypeInt || curAttrs[i].type == TypeReal)
				bufferpos += 4;
			else
				bufferpos += 4 + nchars;
		}
	}
	bufferpos = -1;
	pos = -1;
	return -1;
}

//////////////////////////////////////////////////////////////
// HashJoin function definitions
//////////////////////////////////////////////////////////////

HashJoin::HashJoin(Iterator *leftIn, Iterator *rightIn, const Condition & condition, const unsigned  numPages)
	: _hash(NULL), _buffer(NULL), R_PREFIX_FILENAME("HashJoinR"), S_PREFIX_FILENAME("HashJoinS"),
	  _currSPartitionIndex(0), _currRFilename("")
{
	_pf = PF_Manager::Instance();

	_R = leftIn;
	_S = rightIn;

	if (_R != NULL && _S != NULL 
		&& condition.bRhsIsAttr 
		&& condition.op == EQ_OP
		&& numPages >= 3)	// 1 page for input buffer + >= 2 pages for partitioning
	{	
		_R->getAttributes(_RAttributes);
		bool foundAttr = false;
		_maxRTupleSize = 0;
		for (unsigned i = 0; i <_RAttributes.size(); ++i)
		{
			_maxRTupleSize += _RAttributes[i].length;
			if (_RAttributes[i].name == condition.lhsAttr)
			{
				_RAttrIndex = i;
				foundAttr = true;
			}
		}
		assert(foundAttr);

		_S->getAttributes(_SAttributes);
		foundAttr = false;
		_maxSTupleSize = 0;
		for (unsigned i = 0; i < _SAttributes.size(); ++i)
		{
			_maxSTupleSize += _SAttributes[i].length;
			if (_SAttributes[i].name == condition.rhsAttr)
			{
				_SAttrIndex = i;
				foundAttr = true;
			}
		}
		assert(foundAttr);

		assert(_SAttributes[_SAttrIndex].type == _RAttributes[_RAttrIndex].type);

		_op = condition.op;

		_numPages = numPages;
		unsigned bufferSize = _numPages * PF_PAGE_SIZE;
		_buffer = new char[bufferSize];

		/////////////////
		// phase 1
		/////////////////

		{
			// TODO: re-partition partitions that have too much data

			// partition S
			unsigned partitionBufferSize = bufferSize - PF_PAGE_SIZE;
			char* partitionBuffer = _buffer + PF_PAGE_SIZE;
			IntegerPartitionHash sHash(partitionBufferSize, partitionBuffer, PF_PAGE_SIZE,
										numeric_limits<int>::min(), numeric_limits<int>::max(),
										S_PREFIX_FILENAME);

			char* tupleBuffer = _buffer;
			const void* itemPtr;	// pointer to "key"
			unsigned dataSize;
			int key;
			bool isInserted;
			while (_S->getNextTuple(tupleBuffer) != QE_EOF)
			{
				GetExternalDataItemPointerAndDataSize(tupleBuffer, _SAttributes, _SAttrIndex,
													  itemPtr, dataSize);

				if ( _SAttributes[_SAttrIndex].type == TypeVarChar )
				{
					string keyData = ExtractString(itemPtr);
					key = HashString(keyData);
				}
				else
					key = *reinterpret_cast<const int*>(itemPtr);

				isInserted = sHash.insert(key, tupleBuffer, dataSize);
				if (!isInserted)
				{
					_numPages = 0;	// indicate error
					return;
				}
			}

			if (!sHash.FlushPartitionsToDisk())
			{
				_numPages = 0;	// indicate error
				return;
			}

			_partitionIntervalLength = sHash.getPartitionIntervalLength();
			_numPartitions = sHash.getNumPartitions();

			if (!sHash.Invalidate())
			{
				_numPages = 0;	// indicate error
				return;
			}
		}

		{
			// partition R
			unsigned partitionBufferSize = bufferSize - PF_PAGE_SIZE;
			char* partitionBuffer = _buffer + PF_PAGE_SIZE;
			IntegerPartitionHash rHash(partitionBufferSize, partitionBuffer, PF_PAGE_SIZE,
										numeric_limits<int>::min(), numeric_limits<int>::max(),
										R_PREFIX_FILENAME);

			char* tupleBuffer = _buffer;
			const void* itemPtr;	// pointer to "key"
			unsigned dataSize;
			int key;
			bool isInserted;
			while (_R->getNextTuple(tupleBuffer) != QE_EOF)
			{
				GetExternalDataItemPointerAndDataSize(tupleBuffer, _RAttributes, _RAttrIndex,
													  itemPtr, dataSize);

				if ( _RAttributes[_RAttrIndex].type == TypeVarChar )
				{
					string keyData = ExtractString(itemPtr);
					key = HashString(keyData);
				}
				else
					key = *reinterpret_cast<const int*>(itemPtr);

				isInserted = rHash.insert(key, tupleBuffer, dataSize);
				if (!isInserted)
				{
					_numPages = 0;	// indicate error
					return;
				}
			}

			if (!rHash.FlushPartitionsToDisk())
			{
				_numPages = 0;	//indicate error
				return;
			}

			assert(_partitionIntervalLength == rHash.getPartitionIntervalLength());
			assert(_numPartitions == rHash.getNumPartitions());

			if (!rHash.Invalidate())
			{
				_numPages = 0;	// indicate error
				return;
			}
		}
	}
	else
		_numPages = 0;
}

HashJoin::~HashJoin()
{
	if (_buffer != NULL)
	{
		delete [] _buffer;
		_buffer = NULL;
	}

	if (_hash != NULL)
	{
		delete _hash;
		_hash = NULL;
	}
}

RC HashJoin::getNextTuple(void* data)
{
	if (isProperlyPrepared())
	{		
		// foreach partition
		string sFilename;
		string rFilename;
		bool rFileExists;
		for (; _currSPartitionIndex < _numPartitions; ++_currSPartitionIndex)
		{
			rFileExists = false;

			if (_currRFilename.length() > 0)
			{
				/////////////////////////////////////////////////////////////
				// Read R's corresponding partition and probe the hash table
				/////////////////////////////////////////////////////////////

				void* rTuple;
				unsigned rTupleSize;
				const void* key;
				void const* sTuple;
				unsigned sTupleSize;
				while (_rIter.getPointerToNextTuple(rTuple, rTupleSize))
				{
					GetExternalDataItemPointerAndDataSize(rTuple, _RAttributes, _RAttrIndex,
														  key, rTupleSize); 
			
					if (_hash->find(key, sTuple, sTupleSize))
					{
						memcpy(data, rTuple, rTupleSize);
						memcpy(reinterpret_cast<char*>(data) + rTupleSize, sTuple, sTupleSize);
						return 0;
					}
				}
				assert(_rIter.hasReachedEOF());

				if (_pf->CloseFile(_rFileHandle) != 0)
					assert(false);
				if (_pf->DestroyFile(_currRFilename.c_str()) != 0)
					assert(false);
				
				_currRFilename = "";
			}

			////////////////////////////////////////////////////
			// Put S's partition into the hash table
			////////////////////////////////////////////////////

			sFilename = IntegerPartitionHash::getFilename(S_PREFIX_FILENAME,
															_currSPartitionIndex, _partitionIntervalLength,
															numeric_limits<int>::min());

			rFilename = IntegerPartitionHash::getFilename(R_PREFIX_FILENAME,
														_currSPartitionIndex, _partitionIntervalLength,
														numeric_limits<int>::min());

			rFileExists = doesFileExist(rFilename.c_str());

			if (doesFileExist(sFilename.c_str()) && rFileExists)
			{
				PF_FileHandle sFileHandle;
				if (_pf->OpenFile(sFilename.c_str(), sFileHandle) != 0)
					assert(false);

				unsigned numPages = sFileHandle.GetNumberOfPages();

				// sequentially insert tuple data into buffer
				PartitionIterator itr(sFileHandle, _buffer, PF_PAGE_SIZE,
										_SAttributes, _SAttrIndex);
				void* tupleBuffer;
				unsigned tupleSize;
				char* hashBuffer = _buffer + PF_PAGE_SIZE;
				unsigned hashBufferIndex = 0;
				unsigned bufferSize = _numPages * PF_PAGE_SIZE;
				unsigned maxHashBufferIndex = bufferSize - PF_PAGE_SIZE;
				while (itr.getPointerToNextTuple(tupleBuffer, tupleSize))
				{
					assert(hashBufferIndex + tupleSize <= maxHashBufferIndex);
					memcpy(hashBuffer + hashBufferIndex, tupleBuffer, tupleSize);

					// update hash buffer index
					hashBufferIndex += tupleSize;
				}
				assert(itr.hasReachedEOF());
				if (_pf->CloseFile(sFileHandle) != 0)
					assert(false);
				if (_pf->DestroyFile(sFilename.c_str()) != 0)
					assert(false);

				// foreach tuple hash S to table
				const unsigned hashTableBufferOffset = hashBufferIndex;
				if (maxHashBufferIndex <= hashTableBufferOffset)
				{
					return NOT_ENOUGH_MEMORY;	// not enough memory
				}
				_hash = new Hash(maxHashBufferIndex - hashTableBufferOffset,
								hashBuffer + hashTableBufferOffset, 
								_SAttrIndex, _SAttributes);
				const char* tuplePtr = hashBuffer;
				hashBufferIndex = 0;
				const void* itemPtr;
				unsigned dataSize = 0;
				bool isInserted;
				while (hashBufferIndex < hashTableBufferOffset)
				{
					GetExternalDataItemPointerAndDataSize(tuplePtr, _SAttributes, _SAttrIndex,
															itemPtr, dataSize);

					isInserted = _hash->insert(itemPtr, tuplePtr);
					if (!isInserted)
						return DUPLICATE_KEY_OR_NOT_ENOUGH_MEMORY;

					// update data
					hashBufferIndex += dataSize;
					tuplePtr += dataSize;
				}
			}
			else if (rFileExists)
			{
				// destroy the corresponding R partition file (since S doesn't have one)
				_pf->DestroyFile(rFilename.c_str());

				continue;
			}
			else
			{
				// destroy the S partition file (since R doesn't have a corresponding one)
				_pf->DestroyFile(sFilename.c_str());

				continue;
			}

			/////////////////////////////////////////////////////////////
			// Open R's corresponding partition
			/////////////////////////////////////////////////////////////

			assert(doesFileExist(rFilename.c_str()));

			if (_pf->OpenFile(rFilename.c_str(), _rFileHandle) == 0)
			{
				_rIter = PartitionIterator(_rFileHandle, _buffer, PF_PAGE_SIZE,
										   _RAttributes, _RAttrIndex);

				_currRFilename = rFilename;
			}
			else
				assert(false);
		}

		return QE_EOF;
	}

	return UNKNOWN_ERROR;
}

void HashJoin::getAttributes(vector<Attribute> & attrs) const
{
	attrs = _RAttributes;
	attrs.insert(attrs.end(), _SAttributes.begin(), _SAttributes.end());
}



Aggregate::Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op)
{
}



Aggregate::Aggregate(Iterator *input, Attribute aggAttr, Attribute gAttr, AggregateOp op)
{
}



Aggregate::~Aggregate()
{
}



void Aggregate::getAttributes(vector<Attribute> & attrs) const
{
}

