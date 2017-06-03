#include <linux/kernel.h>
#include <linux/module.h>
#include <linux/vmalloc.h>
#include <ring_buffer_scif.h>
#include <ring_buffer.h>
#include "ring_buffer_i.h"

#define NUM_ITER      10
#define SIZE          1024
#define DATA_BUF_SIZE 8192
#define LOCK_ENABLED

static char *data_buf;
static int is_master = 1;

int unit_test_master(int local_port)
{
	struct ring_buffer_scif_t rbs;
	struct ring_buffer_req_t req;
	int i, rc;

	/* create */
	rc = ring_buffer_scif_create_master(4096,
					    64,
					    RING_BUFFER_BLOCKING,
					    RING_BUFFER_SCIF_PRODUCER,
					    NULL, NULL,
					    &rbs);
	rb_test(rc == 0, "ring_buffer_scif_create_master");
	rb_dump(rbs.rb);

	/* wait for client connection */
	rb_log("wait for shadow...[%d]\n", local_port);
	rc = ring_buffer_scif_wait_for_shadow(&rbs, local_port, 1);
	rb_test(rc == 0, "ring_buffer_scif_wait_for_shadow");

	/* put 10 elements */
	for (i = 0; i < NUM_ITER; ++i) {
		ring_buffer_put_req_init(&req, BLOCKING, SIZE);
#ifdef LOCK_ENABLED
		rc = ring_buffer_scif_put(&rbs, &req);
#else
		rc = ring_buffer_scif_put_nolock(&rbs, &req);
#endif
		rb_test(rc == 0, "ring_buffer_put");

		memset(data_buf, '0' + i, SIZE);
		rc = copy_to_ring_buffer_scif(&rbs, req.data, data_buf, SIZE);
		rb_test(rc == 0, "copy_to_ring_buffer_scif");
		rb_dump(rbs.rb);
		set_current_state(TASK_INTERRUPTIBLE);
		schedule_timeout(1 * HZ);

		ring_buffer_scif_elm_set_ready(&rbs, req.data);
		rb_log("  ==> put data: %c [0x%x]\n", data_buf[0], data_buf[0]);
	}

	/* take a nap while the shadow take the enqueued items */
	set_current_state(TASK_INTERRUPTIBLE);
	schedule_timeout(10 * HZ);

	/* destroy */
	ring_buffer_scif_destroy_master(&rbs);
	rb_test(1, "ring_buffer_scif_destroy_master");
	return 0;
}

int unit_test_shadow(int local_port, int master_node, int remote_port)
{
	struct ring_buffer_scif_t rbs;
	struct ring_buffer_req_t req;
	int i, j, rc;

	/* create */
	rc = ring_buffer_scif_create_shadow(local_port,
					    master_node, remote_port,
					    NULL, NULL,
					    &rbs);
	rb_test(rc == 0, "ring_buffer_scif_create_shadow");
	rb_dump(rbs.rb);

	/* get 10 elements */
	for (i = 0; i < (NUM_ITER+1); ++i) {
		ring_buffer_get_req_init(&req, BLOCKING);
#ifdef LOCK_ENABLED
		rc = ring_buffer_scif_get(&rbs, &req);
#else
		rc = ring_buffer_scif_get_nolock(&rbs, &req);
#endif
		if (i < NUM_ITER) {
			rb_test(rc == 0, "ring_buffer_scif_get");
		}
		else {
			rb_test(rc != 0, "ring_buffer_scif_get");
			break;
		}

		rc = copy_from_ring_buffer_scif(&rbs, data_buf, req.data, req.size);
		schedule_timeout(1 * HZ);
		ring_buffer_scif_elm_set_done(&rbs, req.data);
		rb_log("  ==> get data: %c [0x%x]\n", data_buf[0], data_buf[0]);
		rb_test(data_buf[0] == (char)('0' + i),
			"check ring buffer data");
		for (j = 0; j < SIZE; ++j) {
			if (data_buf[j] != (char)('0' + i))
				break;
		}
		rb_test(j == SIZE, "check ring buffer data thoroughly");
		rb_dump(rbs.rb);
		schedule_timeout(1 * HZ);
	}


	/* destory */
	rb_dump(rbs.rb);
	ring_buffer_scif_destroy_shadow(&rbs);
	rb_test(1, "ring_buffer_scif_destroy_shadow");
	return 0;
}

static int __init pci_ring_buffer_test_init(void)
{
	int rc;

	printk(KERN_ERR "[%s:%d] start <<<\n",
	       __func__, __LINE__);

	data_buf = kmalloc(DATA_BUF_SIZE, GFP_KERNEL);;

	if (is_master)
		rc = unit_test_master(8888);
	else
		rc = unit_test_shadow(8888, 0, 8888);

	printk(KERN_ERR "[%s:%d] finish: %d <<<\n",
	       __func__, __LINE__, rc);
	return rc;
}

static void __exit pci_ring_buffer_test_exit(void)
{
	printk(KERN_ERR "[%s:%d] end >>>\n",
	       __func__, __LINE__);
	kfree(data_buf);
}

module_init(pci_ring_buffer_test_init);
module_exit(pci_ring_buffer_test_exit);

MODULE_DESCRIPTION("PCI ring buffer tester");
MODULE_VERSION("1.0");
MODULE_AUTHOR("Changwoo Min <changwoo@gatech.edu>");

module_param(is_master,   int, 0);
MODULE_PARM_DESC(is_master, "is_master? {0|1}");
/*
 * This code is in the public domain, but in Linux it's simplest to just
 * say it's GPL and consider the authors as the copyright holders.
 */
MODULE_LICENSE("GPL");

