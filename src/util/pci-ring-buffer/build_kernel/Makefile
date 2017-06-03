SRC_ROOT := ..

obj-$(CONFIG_PCI_RING_BUFFER)      += pci_ring_buffer.o
pci_ring_buffer-objs               := $(addprefix $(SRC_ROOT)/lib/, \
				      	ring_buffer_scif.o \
				      	ring_buffer_porting.o)

obj-$(CONFIG_PCI_RING_BUFFER_TEST) += pci_ring_buffer_test.o
pci_ring_buffer_test-objs          := $(addprefix $(SRC_ROOT)/test_kernel/, \
					rbs-blocking-ut.o)

ccflags-y += -I$(src)/../include
ccflags-y += -O3 -mtune=native
EXTRA_CFLAGS += -DRING_BUFFER_CONF_KERNEL \
		-DRING_BUFFER_CONF_NO_MMAP \
		-DRING_BUFFER_CONF_NO_DOUBLE_MMAP \
		-DRING_BUFFER_ASSERT

ifeq ($(CONFIG_PCI_RING_BUFFER_TEST),m)
	ccflags-y += -I$(src)/../lib
	EXTRA_CFLAGS += -DRING_BUFFER_TRACE_EVENT \
			-DRING_BUFFER_TRACE_FINGERPRINT \
			-DRING_BUFFER_DEBUG
endif
