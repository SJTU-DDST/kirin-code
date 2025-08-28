#include <stdio.h>
#include <string.h>
#include "ext4_cs.h"
#include "cs_io.h"
#include "memory_map.h"
#include "nvme/debug.h"
#include "xil_printf.h"
#include "shared_mem.h"
#include "utils_common.h"


#define BLOCK_BUF_SIZE 4096
#define GLOBAL_BLOCK_BUF_SIZE (32 * BLOCK_BUF_SIZE)
static volatile uint8_t *global_block_buf;
static volatile struct ext4_sb *global_sb;
static volatile struct ext4_group_desc *global_gd;
static volatile struct ext4_inode *global_inode;

/* these are indexed by sqe->file_idx passed from CPU 1 */
static struct cs_file *cs_files;

static volatile struct shared_mem *m = (struct shared_mem *)SHARED_MEM_BASE_ADDR;

static inline int64_t __ext4_block_size(int s_log_block_size)
{
    return 1 << (10 + s_log_block_size);
}

static inline struct ext4_extent *ee_array(struct ext4_extent_header *eh)
{
    return (struct ext4_extent *)((void *)eh + sizeof(struct ext4_extent_header));
}

static inline struct ext4_extent_idx *ei_array(struct ext4_extent_header *eh)
{
    return (struct ext4_extent_idx *)((void *)eh + sizeof(struct ext4_extent_header));
}

static int ext4_get_inode(uint64_t inode_nr, volatile struct ext4_inode *inode, struct cs_file *file)
{
    uint64_t bg_nr, inode_offset;
    uint64_t gd_offset;
    struct cs_io_handle handle;

    if (!m->fs_ready)
        return -1;

    bg_nr = (inode_nr - 1) / global_sb->s_inodes_per_group;

    gd_offset = __ext4_block_size(global_sb->s_log_block_size) + bg_nr * global_sb->s_desc_size;
    if (__ext4_block_size(global_sb->s_log_block_size) == 1024) {
        /* superblock is at block 1 in this case */
        gd_offset += 1024;
    }
    FLUSH_CACHE(global_gd, sizeof(struct ext4_group_desc));
    handle = read_from_storage(global_gd, gd_offset, sizeof(struct ext4_group_desc), NULL);
    sync_cs_io_req(&handle);

    /* inode table offset */
    inode_offset = ((uint64_t)global_gd->bg_inode_table_lo + ((uint64_t)global_gd->bg_inode_table_hi << 32)) *
            __ext4_block_size(global_sb->s_log_block_size);
    /* adjust within inode table */
    inode_offset += ((inode_nr - 1) % global_sb->s_inodes_per_group) * global_sb->s_inode_size;

    FLUSH_CACHE(inode, sizeof(struct ext4_inode));
    handle = read_from_storage(inode, inode_offset, sizeof(struct ext4_inode), NULL);
    sync_cs_io_req(&handle);

    return 0;
}


/* make sure eh points to a contiguous buffer in memory */
static struct cs_file *ext4_collect_extents_recursive(struct ext4_extent_header *eh,
        uint64_t block_size, struct cs_file *file, volatile uint8_t *block_buf)
{
    struct cs_io_handle handle;

    ASSERT(block_buf + BLOCK_BUF_SIZE <= global_block_buf + GLOBAL_BLOCK_BUF_SIZE);

    if (block_size > BLOCK_BUF_SIZE) {
        xil_printf("Need to increase block data buffer size to %u\n", (uint32_t)block_size);
        ASSERT(0);
    }

    if (eh->eh_depth == 0) {
        /* leaf level */
        struct ext4_extent *ees = ee_array(eh);
        uint64_t ees_size = sizeof(struct ext4_extent) * eh->eh_entries;

        if (file->index_size > 0) {
            struct ext4_extent *prev_ee = (struct ext4_extent *)(file->index + file->index_size -
                    sizeof(struct ext4_extent));

            ASSERT(prev_ee->ee_len <= 0x8000);
            if (prev_ee->ee_block + prev_ee->ee_len != ees[0].ee_block)
                xil_printf("Found a hole in the extent tree: %u\n", prev_ee->ee_block + prev_ee->ee_len);
        }

        ASSERT(CONFIG_CS_FILE_INDEX_DEFAULT_LEN >= file->index_size + ees_size);

        memcpy(&file->index[file->index_size], ees, ees_size);
        file->index_size += ees_size;

        return file;
    } else {
        /* index level */
        struct ext4_extent_idx *eis = ei_array(eh);

        for (int i = 0; i < eh->eh_entries; i++) {
            uint64_t block_nr = (uint64_t)eis[i].ei_leaf_lo + ((uint64_t)eis[i].ei_leaf_hi << 32);

            FLUSH_CACHE(block_buf, block_size);
            handle = read_from_storage(block_buf, block_nr * block_size, block_size, NULL);
            sync_cs_io_req(&handle);

            file = ext4_collect_extents_recursive((struct ext4_extent_header *)block_buf,
                    block_size, file, block_buf + BLOCK_BUF_SIZE);
        }

        return file;
    }
}

static struct cs_file *ext4_collect_extents(struct cs_file *file, volatile struct ext4_inode *inode)
{
    if (inode->i_flags & 0x00080000) {
        /* extents */
        return ext4_collect_extents_recursive((struct ext4_extent_header *)inode->i_block,
                __ext4_block_size(global_sb->s_log_block_size), file, global_block_buf);
    }

    if (inode->i_flags & 0x10000000) {
        /* inlined data */
        xil_printf("Inlined data not supported yet\n");
    } else {
        xil_printf("We don't support this kind of inode for the moment: %u\n", inode->i_flags);
    }

    return NULL;
}

int64_t ext4_bg_offset(uint64_t bg_nr)
{
    if (!m->fs_ready)
        return -1;

    return bg_nr * global_sb->s_blocks_per_group * __ext4_block_size(global_sb->s_log_block_size);
}

int64_t ext4_block_count()
{
    if (!m->fs_ready)
        return -1;

    return (int64_t)global_sb->s_blocks_count_lo + ((int64_t)global_sb->s_blocks_count_hi << 32);
}

/* make sure eh points to a contiguous buffer in memory */
static void ext4_walk_extent_tree_recursive(struct ext4_extent_header *eh, uint64_t *sum,
        uint64_t *p, uint64_t block_size, uint64_t file_size, volatile uint8_t *block_buf)
{
    struct cs_io_handle handle;

    ASSERT(block_buf + BLOCK_BUF_SIZE <= global_block_buf + GLOBAL_BLOCK_BUF_SIZE);

    if (block_size > BLOCK_BUF_SIZE) {
        xil_printf("Need to increase block data buffer size to %u\n", (unsigned int)block_size);
        ASSERT(0);
    }

    if (eh->eh_depth == 0) {
        /* leaf level */
        struct ext4_extent *ees = ee_array(eh);

        for (int i = 0; i < eh->eh_entries; i++) {
            uint64_t block_nr = (uint64_t)ees[i].ee_start_lo + ((uint64_t)ees[i].ee_start_hi << 32);
            uint64_t offset = block_nr * block_size;
            uint64_t nbytes = (ees[i].ee_len > 0x8000 ? ees[i].ee_len - 0x8000 : ees[i].ee_len) * block_size;

            if (ees[i].ee_len > 0x8000)
                xil_printf("Extent marked as uninitialized: %u\n", ees[i].ee_block);

            if (ees[i].ee_block * block_size != *p)
                xil_printf("Looks like the extent tree is not sorted\n");

            while (nbytes > 0) {
                FLUSH_CACHE(block_buf, block_size);
                handle = read_from_storage(block_buf, offset, block_size, NULL);
                sync_cs_io_req(&handle);

                for (uint64_t i = 0; i < block_size; i++)
                    *sum += block_buf[i];
                *p += block_size;

                offset += block_size;
                nbytes -= block_size;
            }
        }
    } else {
        /* index level */
        struct ext4_extent_idx *eis = ei_array(eh);

        for (int i = 0; i < eh->eh_entries; i++) {
            uint64_t block_nr = (uint64_t)eis[i].ei_leaf_lo + ((uint64_t)eis[i].ei_leaf_hi << 32);

            FLUSH_CACHE(block_buf, block_size);
            handle = read_from_storage(block_buf, block_nr * block_size, block_size, NULL);
            sync_cs_io_req(&handle);

            ext4_walk_extent_tree_recursive((struct ext4_extent_header *)block_buf,
                    sum, p, block_size, file_size, block_buf + BLOCK_BUF_SIZE);
        }
    }
}

void ext4_walk_extent_tree(volatile struct ext4_inode *inode)
{
    uint64_t sum = 0;
    uint64_t file_size = (uint64_t)inode->i_size_lo + ((uint64_t)inode->i_size_high << 32);
    uint64_t p = 0;

    if (inode->i_flags & 0x10000000) {
        /* inlined data */
        if (file_size > sizeof(inode->i_block)) {
            xil_printf("A little odd that this inode has inlined data but file size exceeds 60\n");
            file_size = sizeof(inode->i_block);
            while (p < file_size) {
                sum += ((uint8_t *)inode->i_block)[p];
                p++;
            }
        }
    } else if (inode->i_flags & 0x00080000) {
        /* extents */
        ext4_walk_extent_tree_recursive((struct ext4_extent_header *)inode->i_block, &sum, &p,
                __ext4_block_size(global_sb->s_log_block_size), file_size, global_block_buf);
    } else {
        xil_printf("We don't support this kind of inode for the moment: %u\n", inode->i_flags);
    }

    xil_printf("Sum of all bytes in the file is 0x%08x%08x\n", (uint32_t)(sum >> 32), (uint32_t)sum);
}

int ext4_check_inode(uint64_t inode_nr)
{
    int ret = ext4_get_inode(inode_nr, global_inode, NULL);

    if (ret != 0)
        return ret;

    xil_printf("File size: %08x%08x\n", global_inode->i_size_high, global_inode->i_size_lo);

    ext4_walk_extent_tree(global_inode);

    return 0;
}

struct cs_file *ext4_file_open(struct cs_file *file)
{
    ASSERT(file != NULL);

    if (ext4_get_inode(file->id, global_inode, file) != 0) {
        return NULL;
    }

    file->size = (uint64_t)global_inode->i_size_lo + ((uint64_t)global_inode->i_size_high << 32);

    return ext4_collect_extents(file, global_inode);
}

static int alloc_cs_file_req(struct cs_file *file)
{
    for (int i = 0; i < CONFIG_CS_FILE_MAX_PENDING_REQS; i++)
        if (file->file_reqs[i].req_type == -1) {
            ASSERT(file->file_reqs[i].file == file);
            ASSERT(file->file_reqs[i].pending_ios == 0);
            return i;
        }

    return -1;
}

extern struct host_io_req *alloc_host_io_req();

uint64_t ext4_file_io(struct cs_file *file, void *buf, uint64_t len, uint64_t offset, int is_read)
{
    struct ext4_extent *ees = (struct ext4_extent *)file->index;
    int nr_ees = file->index_size / sizeof(struct ext4_extent);
    uint64_t block_size = __ext4_block_size(global_sb->s_log_block_size);
    uint64_t done;
    int req_idx;

    req_idx = alloc_cs_file_req(file);
    if (req_idx < 0)
        return 0;

    file->file_reqs[req_idx].req_type = is_read ? FILE_REQ_READ : FILE_REQ_WRITE;
    file->file_reqs[req_idx].pending_ios = 0;

    /* clumsy */
    done = 0;
    for (int i = 0; i < nr_ees; i++) {
        uint64_t file_offset, disk_offset, extent_len, io_len;
        struct ext4_extent content;

        memcpy(&content, &ees[i], sizeof(struct ext4_extent));

        file_offset = content.ee_block * block_size;
        extent_len = content.ee_len * block_size;

        ASSERT(content.ee_len <= 0x8000);

        if (file_offset <= offset + done && file_offset + extent_len > offset + done) {
            disk_offset = (content.ee_start_lo + ((uint64_t)content.ee_start_hi << 32)) * block_size;
            disk_offset += offset + done - file_offset;
            io_len = file_offset + extent_len - (offset + done);
            if (io_len > len - done)
                io_len = len - done;

            file->file_reqs[req_idx].pending_ios++;

            done += io_len;
            if (done >= len)
                break;
        }
    }
    ASSERT(done == len);

    done = 0;
    for (int i = 0; i < nr_ees; i++) {
        uint64_t file_offset, disk_offset, extent_len, io_len;
        struct ext4_extent content;

        memcpy(&content, &ees[i], sizeof(struct ext4_extent));

        file_offset = content.ee_block * block_size;
        extent_len = content.ee_len * block_size;

        ASSERT(content.ee_len <= 0x8000);

        if (file_offset <= offset + done && file_offset + extent_len > offset + done) {
            disk_offset = (content.ee_start_lo + ((uint64_t)content.ee_start_hi << 32)) * block_size;
            disk_offset += offset + done - file_offset;
            io_len = file_offset + extent_len - (offset + done);
            if (io_len > len - done)
                io_len = len - done;
            
#if 1
            struct host_io_req *host_io_req = alloc_host_io_req();
            struct emu_req_sqe *sqe;
    
            host_io_req->is_read = is_read;
            host_io_req->is_cs = true;
            host_io_req->file_req = &file->file_reqs[req_idx];
            host_io_req->slba = disk_offset / BYTES_PER_NVME_BLOCK;
            host_io_req->nlb = (disk_offset + io_len - 1) / BYTES_PER_NVME_BLOCK -
                                disk_offset / BYTES_PER_NVME_BLOCK + 1;
            sqe = qpair_alloc_sqe(&m->emu_req_qp);
            sqe->host_io_req = host_io_req;
            qpair_submit_sqe(&m->emu_req_qp, sqe);
#else
            file->file_reqs[req_idx].pending_emus = 0;
#endif
    

            if (is_read)
                read_from_storage(buf + done, disk_offset, io_len, &file->file_reqs[req_idx]);
            else
                write_to_storage(buf + done, disk_offset, io_len, &file->file_reqs[req_idx]);

            done += io_len;
            if (done >= len)
                break;
        }
    }
    ASSERT(done == len);

    return len;
}

int __attribute__((optimize("O0"))) ext4_probe(int is_ready)
{
    static volatile struct ext4_sb *local_sb = NULL;
    struct cs_io_handle handle;

    if (local_sb == NULL) {
        local_sb = linear_malloc(sizeof(struct ext4_sb), 4096);
        assert(local_sb != NULL);
    }

    m->fs_ready = is_ready;

    if (!is_ready)
        return 0;

    /* according to the ext4 doc, there is a 1KB padding in block group 0 */
    /* we are not dealing with partitioned drives, so block group 0 sits at the beginning */
    FLUSH_CACHE(local_sb, sizeof(struct ext4_sb));
    handle = read_from_storage(local_sb, 1024, sizeof(struct ext4_sb), NULL);
    sync_cs_io_req(&handle);
    if (local_sb->s_magic == 0xef53) {
        *global_sb = *local_sb;

        xil_printf("Found ext4 superblock\n");
        xil_printf("Inode count: %u\n", local_sb->s_inodes_count);
        xil_printf("Inode size: %u\n", local_sb->s_inode_size);
        xil_printf("Inodes per group: %u\n", local_sb->s_inodes_per_group);
        xil_printf("Block count: %lld\n", ext4_block_count());
        xil_printf("Block size: 2 ^ (10 + %u)\n", local_sb->s_log_block_size);
        xil_printf("Blocks per group: %u\n", local_sb->s_blocks_per_group);
        xil_printf("Compatible feature set: 0x%08x\n", local_sb->s_feature_compat);
        xil_printf("Incompatible feature set: 0x%08x\n", local_sb->s_feature_incompat);
        xil_printf("Read-only compatible feature set: 0x%08x\n", local_sb->s_feature_ro_compat);

        if (local_sb->s_feature_incompat & 0x200)
            xil_printf("Beware this ext4 has flexible block group (2 ^ %d groups per flex bg)\n",
                    (int)local_sb->s_log_groups_per_flex);

        if (local_sb->s_feature_ro_compat & 0x8)
            xil_printf("Beware this ext4 records file size in logical blocks (not 512B sectors)\n");

        if (local_sb->s_desc_size == 32) {
            xil_printf("Why are we using this ancient 32B group descriptor format?\n");
            ASSERT(0);
        }

        if (!(local_sb->s_feature_incompat & 0x40)) {
            xil_printf("For now we assume extents are being used\n");
            ASSERT(0);
        }

        FLUSH_CACHE(local_sb, sizeof(struct ext4_sb));
        handle = read_from_storage(local_sb, ext4_bg_offset(7), sizeof(struct ext4_sb), NULL);
        sync_cs_io_req(&handle);
        if (local_sb->s_magic == 0xef53) {
            xil_printf("Found backup superblock in block group 7; everything seems to be fine.\n");
        } else {
            xil_printf("Didn't find backup superblock in block group 7; maybe something's wrong?\n");
        }

        return 0;
    }

    /* didn't find superblock */
    m->fs_ready = 0;

    return -1;
}

void get_file_address(uint64_t id, unsigned int table_num, int is_args_file, int is_result_file)
{
    ASSERT(m->fs_ready);

    struct cs_file file;
	file.id = id;
	file.size = 0;
	file.index_size = 0;

	ASSERT(ext4_file_open(&file) != NULL);

	struct ext4_extent *ees = (struct ext4_extent *)file.index;
    int nr_ees = file.index_size / sizeof(struct ext4_extent);
    uint64_t block_size = __ext4_block_size(global_sb->s_log_block_size);
    if(nr_ees > 1)
    {
    	printf("nr_ees = %d\n", nr_ees);
    }
    for (int i = 0; i < nr_ees; i++) {
        uint64_t file_offset, disk_offset;
        struct ext4_extent content;

        memcpy(&content, &ees[i], sizeof(struct ext4_extent));

        file_offset = content.ee_block * block_size;

        ASSERT(content.ee_len <= 0x8000);

        disk_offset = (content.ee_start_lo + ((uint64_t)content.ee_start_hi << 32)) * block_size - file_offset;
        file.disk_offset = disk_offset;
    }
    if(is_args_file)
        m->args_file_disk_offset = file.disk_offset / (512 / 8);
    else if(is_result_file)
	    m->result_file_disk_offset = file.disk_offset / (512 / 8);
    else
        m->file_disk_offsets[table_num] = file.disk_offset / (512 / 8);
}

static struct cs_file *cs_file_open(uint64_t id, int file_idx)
{
    struct cs_file *ret;

    ASSERT(m->fs_ready);

    ret = &cs_files[file_idx];

    ret->id = id;
    ret->size = 0;
    ret->index_size = 0;

    for (int i = 0; i < CONFIG_CS_FILE_MAX_PENDING_REQS; i++) {
        ASSERT(ret->file_reqs[i].file == ret);
        ASSERT(ret->file_reqs[i].req_type == -1);
        ASSERT(ret->file_reqs[i].pending_ios == 0);
    }

    ASSERT(ext4_file_open(ret) != NULL);

    struct ext4_extent *ees = (struct ext4_extent *)ret->index;
    int nr_ees = ret->index_size / sizeof(struct ext4_extent);
    uint64_t block_size = __ext4_block_size(global_sb->s_log_block_size);
    if(nr_ees > 1)
    {
    	printf("nr_ees = %d\n", nr_ees);
    }
    for (int i = 0; i < nr_ees; i++) {
        uint64_t file_offset, disk_offset;
        struct ext4_extent content;

        memcpy(&content, &ees[i], sizeof(struct ext4_extent));

        file_offset = content.ee_block * block_size;

        ASSERT(content.ee_len <= 0x8000);

        disk_offset = (content.ee_start_lo + ((uint64_t)content.ee_start_hi << 32)) * block_size - file_offset;
        ret->disk_offset = disk_offset;
    }
    return ret;
}

static uint64_t cs_file_io(struct cs_file *file, void *buf, uint64_t len, uint64_t offset, int is_read)
{
    ASSERT(len > 0);
    ASSERT(len + offset <= file->size);

    return ext4_file_io(file, buf, len, offset, is_read);
}

void process_sq()
{
    struct file_req_sqe *sqe;
    struct file_req_cqe *cqe;
    struct cs_file *file;
    uint64_t ret;

    while ((sqe = qpair_peek_sqe(&m->file_req_qp)) != NULL) {
        switch (sqe->req_type) {
        case FILE_REQ_OPEN:
            /* process 1 open request at a time and post cqe directly */
            /* TODO: asynchronous open */
            file = cs_file_open(sqe->file_id, sqe->file_idx);

            cqe = qpair_alloc_cqe(&m->file_req_qp);
            cqe->file_idx = sqe->file_idx;
            cqe->req_type = FILE_REQ_OPEN;
            cqe->size = file->size;
            cqe->disk_offset = file->disk_offset;
            qpair_submit_cqe(&m->file_req_qp, cqe);

            qpair_consume_sqe(&m->file_req_qp, sqe);

            return;

        case FILE_REQ_CLOSE:
            /* close does not need any processing at the moment */
            break;

        case FILE_REQ_READ:
        case FILE_REQ_WRITE:
            /* cqe is submitted asynchronously */
            ret = cs_file_io(&cs_files[sqe->file_idx], sqe->buf, sqe->length, sqe->offset, sqe->req_type == FILE_REQ_READ);
            if (ret == 0) {
                /* can't process this sqe at the moment */
                return;
            }
            break;
        default:
            ASSERT(0);
        }

        qpair_consume_sqe(&m->file_req_qp, sqe);
    }
}

static void submit_file_req_cq(struct cs_file_req *file_req)
{
    struct file_req_cqe *cqe = qpair_alloc_cqe(file_req->file->qp);

    cqe->file_idx = file_req->file - cs_files;
    cqe->req_type = file_req->req_type;
    cqe->req_seq = file_req->req_seq;

    qpair_submit_cqe(file_req->file->qp, cqe);

    file_req->req_type = -1;
}

void signal_cs_io_req_done(struct cs_file_req *file_req)
{
    ASSERT(file_req->pending_ios > 0);
    ASSERT(file_req->req_type == FILE_REQ_READ || file_req->req_type == FILE_REQ_WRITE);

    file_req->pending_ios--;
    if (file_req->pending_ios == 0 && file_req->pending_emus == 0)
        submit_file_req_cq(file_req);
}

void signal_cs_emu_req_done(struct cs_file_req *file_req)
{
    ASSERT(file_req->pending_emus > 0);
    ASSERT(file_req->req_type == FILE_REQ_READ || file_req->req_type == FILE_REQ_WRITE);

    file_req->pending_emus--;
    if (file_req->pending_ios == 0 && file_req->pending_emus == 0)
        submit_file_req_cq(file_req);
}

void init_cs_files()
{
    global_block_buf = linear_malloc(GLOBAL_BLOCK_BUF_SIZE, 4096);
    global_sb = linear_malloc(sizeof(struct ext4_sb), 4096);
    global_gd = linear_malloc(4096, 4096);
    global_inode = linear_malloc(4096, 4096);
    cs_files = linear_malloc(CONFIG_NR_CS_FILES * sizeof(struct cs_file), 0);

    assert(global_block_buf != NULL);
    assert(global_sb != NULL);
    assert(global_gd != NULL);
    assert(global_inode != NULL);
    assert(cs_files != NULL);

    memset(cs_files, 0, CONFIG_NR_CS_FILES * sizeof(struct cs_file));
    for (int i = 0; i < CONFIG_NR_CS_FILES; i++) {
        for (int j = 0; j < CONFIG_CS_FILE_MAX_PENDING_REQS; j++) {
            cs_files[i].file_reqs[j].file = &cs_files[i];
            cs_files[i].file_reqs[j].req_type = -1;
            cs_files[i].file_reqs[j].pending_ios = 0;
        }
    }
}
