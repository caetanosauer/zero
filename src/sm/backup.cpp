/*
 * (c) Copyright 2011-2014, Hewlett-Packard Development Company, LP
 */

#include "w_defines.h"

#include "backup.h"
#include "generic_page.h"
#include "vid_t.h"

#include <boost/static_assert.hpp>
#include <cstdio>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#define BACKUP_MEM_ALIGNMENT 4096
#define BACKUP_ALIGN(address) ( ((((unsigned int)(address)+BACKUP_MEM_ALIGNMENT-1)/BACKUP_MEM_ALIGNMENT)*BACKUP_MEM_ALIGNMENT))
#define BACKUP_ALIGN_DOWN(address) ( ((((unsigned int)(address)/BACKUP_MEM_ALIGNMENT)*BACKUP_MEM_ALIGNMENT))

backup_m::backup_m() {

}

backup_m::~backup_m() {

}

bool backup_m::backup_m_valid(){
	return true;
}

w_rc_t backup_m::retrieve_page(generic_page *page, volid_t vid, shpid_t shpid) {
	char fname[30];

	int k = sprintf(fname, "backup_%d", vid);
	w_assert1(k > 0);
/*
 *	ifstream backup(fname, ios::binary);
 *	w_assert1(backup.is_open());
 *	backup.seekg(sizeof(generic_page)*shpid);
 *	backup.read((char *)page, sizeof(generic_page));
 *	backup.close();
 */
	int fd = open(fname, O_RDONLY|O_DIRECT|O_NOATIME);
	if (fd == -1)
		return RC (eBADCHECKSUM);
	int aligned_pagesize = (int)BACKUP_ALIGN(sizeof(generic_page));
	char *backup_pages = (char *)malloc(aligned_pagesize*2 + BACKUP_MEM_ALIGNMENT);
	char *aligned = (char *)BACKUP_ALIGN(backup_pages);
	int aligned_offset = (int)BACKUP_ALIGN_DOWN(shpid*sizeof(generic_page));
	lseek(fd, aligned_offset, SEEK_SET);
	read(fd, aligned, aligned_pagesize);
	memcpy((char *)page, aligned + (shpid*sizeof(generic_page) - aligned_offset), sizeof(generic_page));
	free(backup_pages);
	
	uint32_t checksum = page->calculate_checkup();
	if (checksum != page->checksum)
		return RC (eBADCHECKSUM);

	if ((page->pid.page != shpid) || (page->pid.vol() != vid))
		return RC (eBADCHECKSUM);

	return RCOK;
}
