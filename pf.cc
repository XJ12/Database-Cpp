#include "pf.h"
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <sys/stat.h>

///////////////////////////////////////////
// Static variables
///////////////////////////////////////////
static const char TAG[] = "PF_HEADER struct tag";
static const int TAG_SIZE = sizeof(TAG);

///////////////////////////////////////////
// Class Definitions
///////////////////////////////////////////

struct PF_Header
{
public:
	unsigned int num_pages;

private:
	char tag[TAG_SIZE];

public:

	PF_Header()
	{
		strcpy(tag, TAG);

		num_pages = 0;
	}

	const char* GetTag() const
	{
		return tag;
	}
};

struct PF_FileHandle_Data
{
	PF_Header header;
	FILE* pFile;
};

///////////////////////////////////////////
// Variables
///////////////////////////////////////////
PF_Manager* PF_Manager::_pf_manager = 0;

///////////////////////////////////////////
// Helper Function Declarations
///////////////////////////////////////////

bool DoesFileExist(const char* fileName);

///////////////////////////////////////////
// Class Function Definitions
///////////////////////////////////////////

PF_Manager* PF_Manager::Instance()
{
    if(!_pf_manager)
        _pf_manager = new PF_Manager();
    
    return _pf_manager;    
}


PF_Manager::PF_Manager()
{
}


PF_Manager::~PF_Manager()
{
}

    
RC PF_Manager::CreateFile(const char *fileName)
{
	// check if file exists
	if (DoesFileExist(fileName))
		return -1;	// return error

	// attempt to create file
	FILE * pFile;
	pFile = fopen (fileName, "wb");
	if (pFile == NULL)
		return -1;	// return error
	else
	{
		// write header
		fseek(pFile, 0, SEEK_SET);
		PF_Header header;
		int amt_written = fwrite(&header, 1, sizeof(header), pFile);
		assert(amt_written == sizeof(header));

		// close file
		fclose(pFile);
		pFile = NULL;
	}

    return 0;
}


RC PF_Manager::DestroyFile(const char *fileName)
{
	if (fileName == NULL)
		return -1;

    return remove(fileName);
}


RC PF_Manager::OpenFile(const char *fileName, PF_FileHandle &fileHandle)
{
	// check that fileHandle is already created
	if (&fileHandle == NULL)
		return -1;	// return error

	// check that fileHandle is not already a handle for another opened file
	if (fileHandle._pimpl->pFile != NULL)
		return -1;	// return error

	// check that file exists
	FILE* pFile;
	pFile = fopen(fileName, "r+b");
	if (pFile == NULL)
		return -1;	// return error

	// check that the file is a valid page file
	int seek_result = fseek(pFile, 0, SEEK_SET);
	assert(seek_result == 0);
	PF_Header header;
	int amt_read = fread(&header, 1, sizeof(header), pFile);
	if ((amt_read != sizeof(header))
		|| (strcmp(header.GetTag(), TAG) != 0))
	{
		fclose(pFile);
		return -1;	// return error
	}

	// assign opened file to fileHandle
	fileHandle._pimpl->header = header;
	fileHandle._pimpl->pFile = pFile;

    return 0;
}


RC PF_Manager::CloseFile(PF_FileHandle &fileHandle)
{
	// check that fileHandle is not NULL
	if (&fileHandle == NULL)
		return -1;

	// close file
	int result = fclose(fileHandle._pimpl->pFile);
	if (result != EOF)
		fileHandle._pimpl->pFile = NULL;

    return result;
}


PF_FileHandle::PF_FileHandle()
{
	_pimpl = new PF_FileHandle_Data();
	_pimpl->pFile = NULL;
}
 

PF_FileHandle::~PF_FileHandle()
{
	assert(_pimpl != NULL);

	if (_pimpl->pFile != NULL)
	{
		fclose(_pimpl->pFile );
		_pimpl->pFile = NULL;
	}

	delete _pimpl;
}


RC PF_FileHandle::ReadPage(PageNum pageNum, void *data)
{
	// check that a file is opened
	if (_pimpl->pFile == NULL)
		return -1;

	// set read pointer
	PF_Header& header = _pimpl->header;
	if (pageNum >= header.num_pages)
		return -1;	// return error
	const int HEADER_SIZE = sizeof(header);
	int page_offset = pageNum * PF_PAGE_SIZE;
	fseek(_pimpl->pFile, HEADER_SIZE + page_offset, SEEK_SET);

	// read page
	fread(data, 1, PF_PAGE_SIZE, _pimpl->pFile);

    return 0;
}


RC PF_FileHandle::WritePage(PageNum pageNum, const void *data)
{
	// check that a file is opened
	if (_pimpl->pFile == NULL)
		return -1;

	// check if the page already exists
	PF_Header& header = _pimpl->header;
	if (pageNum >= header.num_pages)
	{
		return -1;	// return error
	}

	// write page
	const int HEADER_SIZE = sizeof(header);
	int page_offset = pageNum * PF_PAGE_SIZE;
	fseek(_pimpl->pFile, HEADER_SIZE + page_offset, SEEK_SET);
	int amt_written = fwrite(data, 1, PF_PAGE_SIZE, _pimpl->pFile);
	if (amt_written != PF_PAGE_SIZE)
		return -1;	// return error

	// flush write operations
	int flush_result = fflush(_pimpl->pFile);
	assert(flush_result == 0);

    return 0;
}


RC PF_FileHandle::AppendPage(const void *data)
{
	// check that a file is opened
	if (_pimpl->pFile == NULL)
		return -1;

	// write page
	PF_Header& header = _pimpl->header;
	fseek(_pimpl->pFile, 0, SEEK_END);
	int amt_written = fwrite(data, 1, PF_PAGE_SIZE, _pimpl->pFile);
	if (amt_written != PF_PAGE_SIZE)
		return -1;	// return error

	// update and write header
	header.num_pages += 1;
	fseek(_pimpl->pFile, 0, SEEK_SET);
	amt_written = fwrite(&header, 1, sizeof(header), _pimpl->pFile);
	assert(amt_written == sizeof(header));

	// flush write operations
	int flush_result = fflush(_pimpl->pFile);
	assert(flush_result == 0);

    return 0;
}


unsigned PF_FileHandle::GetNumberOfPages()
{
	// check that a file is opened
	assert(_pimpl->pFile != NULL);

	return _pimpl->header.num_pages;
}

///////////////////////////////////////////
// Helper Function Definitions
///////////////////////////////////////////

bool DoesFileExist(const char* fileName)
{
	struct stat stFileInfo;

	if(stat(fileName, &stFileInfo) == 0) return true;
	else return false;
}

