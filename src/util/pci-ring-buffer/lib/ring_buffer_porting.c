#if !defined(MOSAIC_HOST_ONLY)
#include <ring_buffer_scif.h>
#endif
#include "ring_buffer_porting.h"
#include "ring_buffer_i.h"

#ifdef RING_BUFFER_CONF_KERNEL
void *__rbs_scif_mmap(void *addr, size_t len, int prot_flags,
		      int map_flags, scif_epd_t epd, off_t offset,
		      struct mmap_info_t *mmap_info)
{
	struct scif_range *range = NULL;
	struct vm_struct *area = NULL;
	unsigned long virt_addr;
	dma_addr_t dma_addr;
	int i, rc;

	/* get DMA address for a given virtual address, addr */
	rc = scif_get_pages(epd, offset, len, &mmap_info->range);
	if (rc)
		goto err_out;
	range = mmap_info->range;

	/* allocate a kernel virtual address range */
	area = alloc_vm_area(len);
	if (!area)
		goto err_out;
	virt_addr = (unsigned long)area->addr;

	/* ioremap the DMA ranges to the allocated virtual address */
	for (i = 0; i < range->nr_pages; ++i, virt_addr += PAGE_SIZE) {
		dma_addr = range->phys_addr[i];
		rc = ioremap_page_range(virt_addr,
					virt_addr + PAGE_SIZE,
					dma_addr,
					PAGE_KERNEL_IO_UC_MINUS);
		if (rc)
			goto err_out;
	}
	return area->addr;
err_out:
	rb_err("Fail to scif_mmap: addr: %p   len: %lu   offset: %lu",
	       addr, len, offset);
	if (area)
		iounmap(area->addr);
	if (range)
		scif_put_pages(range);
	return NULL;
}

int __rbs_scif_munmap(void *addr, size_t len, struct mmap_info_t *mmap_info)
{
	if (addr)
		iounmap(addr);
	if (mmap_info)
		scif_put_pages(mmap_info->range);
	return 0;
}

static inline
int __rbs_is_kernel_addr(void *addr)
{
	/* XXX: x86-64 specific code */
	return (unsigned long)addr > 0xF000000000000000;
}

static
int __rbs_scif_dma_cpy(int op,
		       scif_epd_t epd, void *addr, size_t len, off_t roffset,
		       int rma_flags)
{
	size_t align_len    = (len + ~PAGE_MASK)  & PAGE_MASK;
	void  *align_addr   = (unsigned long)addr & PAGE_MASK;
	size_t align_offset = addr - align_addr;
	scif_pinned_pages_t pinned_pages = NULL;
	off_t loffset= -1;
	int rc;

	/* pin pages */
	rc = scif_pin_pages(align_addr, align_len,
			    SCIF_PROT_READ | SCIF_PROT_WRITE,
			    __rbs_is_kernel_addr(addr) ? SCIF_MAP_KERNEL : 0,
			    &pinned_pages);
	if (rc)
		goto err_out;

	/* register pinned pages */
	loffset = scif_register_pinned_pages(epd, pinned_pages, 0, 0);
	if (loffset < 0) {
		rc = loffset;
		goto err_out;
	}

	/* perform DMA copy */
	switch(op) {
	case SCIF_PROT_READ:
		rc = scif_readfrom(epd, loffset + align_offset,
				   len, roffset, rma_flags);
		break;
	case SCIF_PROT_WRITE:
		rc = scif_writeto(epd, loffset + align_offset,
				  len, roffset, rma_flags);
		break;
	default:
		rc = -EINVAL;
		break;
	}
	if (rc)
		goto err_out;
out:
	/* unregister pages */
	if (loffset != -1) {
		scif_unregister(epd, loffset, align_len);
		loffset = -1;
	}

	/* unpin pages */
	if (pinned_pages) {
		scif_unpin_pages(pinned_pages);
		pinned_pages = NULL;
	}

	return rc;
err_out:
	rb_err("Fail to scif_vreadfrom: addr: %p   len: %lu   offset: %lu   rc: %d",
	       addr, len, roffset, rc);
	goto out;
}

int __rbs_scif_vwriteto(scif_epd_t epd, void *addr, size_t len, off_t roffset,
			int rma_flags)
{
	return __rbs_scif_dma_cpy(SCIF_PROT_WRITE,
				  epd, addr, len, roffset, rma_flags);
}

int __rbs_scif_vreadfrom(scif_epd_t epd, void *addr, size_t len, off_t roffset,
			 int rma_flags)
{
	return __rbs_scif_dma_cpy(SCIF_PROT_READ,
				  epd, addr, len, roffset, rma_flags);
}
#endif /* RING_BUFFER_CONF_KERNEL */
