/*
 * Memory subsystem support
 *
 * Written by Matt Tolentino <matthew.e.tolentino@intel.com>
 *            Dave Hansen <haveblue@us.ibm.com>
 *
 * This file provides the necessary infrastructure to represent
 * a SPARSEMEM-memory-model system's physical memory in /sysfs.
 * All arch-independent code that assumes MEMORY_HOTPLUG requires
 * SPARSEMEM should be contained here, or in mm/memory_hotplug.c.
 */

#include <linux/module.h>
#include <linux/init.h>
#include <linux/topology.h>
#include <linux/capability.h>
#include <linux/device.h>
#include <linux/memory.h>
#include <linux/kobject.h>
#include <linux/memory_hotplug.h>
#include <linux/mm.h>
#include <linux/mutex.h>
#include <linux/stat.h>
#include <linux/slab.h>

#include <linux/atomic.h>
#include <asm/uaccess.h>

static DEFINE_MUTEX(mem_sysfs_mutex);

#define MEMORY_CLASS_NAME	"memory"

static int sections_per_block;

static inline int base_memory_block_id(int section_nr)
{
	return section_nr / sections_per_block;
}

static int memory_subsys_online(struct device *dev);
static int memory_subsys_offline(struct device *dev);

static struct bus_type memory_subsys = {
	.name = MEMORY_CLASS_NAME,
	.dev_name = MEMORY_CLASS_NAME,
	.online = memory_subsys_online,
	.offline = memory_subsys_offline,
};

static BLOCKING_NOTIFIER_HEAD(memory_chain);

int register_memory_notifier(struct notifier_block *nb)
{
        return blocking_notifier_chain_register(&memory_chain, nb);
}
EXPORT_SYMBOL(register_memory_notifier);

void unregister_memory_notifier(struct notifier_block *nb)
{
        blocking_notifier_chain_unregister(&memory_chain, nb);
}
EXPORT_SYMBOL(unregister_memory_notifier);

static ATOMIC_NOTIFIER_HEAD(memory_isolate_chain);

int register_memory_isolate_notifier(struct notifier_block *nb)
{
	return atomic_notifier_chain_register(&memory_isolate_chain, nb);
}
EXPORT_SYMBOL(register_memory_isolate_notifier);

void unregister_memory_isolate_notifier(struct notifier_block *nb)
{
	atomic_notifier_chain_unregister(&memory_isolate_chain, nb);
}
EXPORT_SYMBOL(unregister_memory_isolate_notifier);

static void memory_block_release(struct device *dev)
{
	struct memory_block *mem = container_of(dev, struct memory_block, dev);

	kfree(mem);
}

unsigned long __weak memory_block_size_bytes(void)
{
	return MIN_MEMORY_BLOCK_SIZE;
}

static unsigned long get_memory_block_size(void)
{
	unsigned long block_sz;

	block_sz = memory_block_size_bytes();

	/* Validate blk_sz is a power of 2 and not less than section size */
	if ((block_sz & (block_sz - 1)) || (block_sz < MIN_MEMORY_BLOCK_SIZE)) {
		WARN_ON(1);
		block_sz = MIN_MEMORY_BLOCK_SIZE;
	}

	return block_sz;
}

/*
 * use this as the physical section index that this memsection
 * uses.
 */

static ssize_t show_mem_start_phys_index(struct device *dev,
			struct device_attribute *attr, char *buf)
{
	struct memory_block *mem =
		container_of(dev, struct memory_block, dev);
	unsigned long phys_index;

	phys_index = mem->start_section_nr / sections_per_block;
	return sprintf(buf, "%08lx\n", phys_index);
}

static ssize_t show_mem_end_phys_index(struct device *dev,
			struct device_attribute *attr, char *buf)
{
	struct memory_block *mem =
		container_of(dev, struct memory_block, dev);
	unsigned long phys_index;

	phys_index = mem->end_section_nr / sections_per_block;
	return sprintf(buf, "%08lx\n", phys_index);
}

/*
 * Show whether the section of memory is likely to be hot-removable
 */
static ssize_t show_mem_removable(struct device *dev,
			struct device_attribute *attr, char *buf)
{
	unsigned long i, pfn;
	int ret = 1;
	struct memory_block *mem =
		container_of(dev, struct memory_block, dev);

	for (i = 0; i < sections_per_block; i++) {
		if (!present_section_nr(mem->start_section_nr + i))
			continue;
		pfn = section_nr_to_pfn(mem->start_section_nr + i);
		ret &= is_mem_section_removable(pfn, PAGES_PER_SECTION);
	}

	return sprintf(buf, "%d\n", ret);
}

/*
 * online, offline, going offline, etc.
 */
static ssize_t show_mem_state(struct device *dev,
			struct device_attribute *attr, char *buf)
{
	struct memory_block *mem =
		container_of(dev, struct memory_block, dev);
	ssize_t len = 0;

	/*
	 * We can probably put these states in a nice little array
	 * so that they're not open-coded
	 */
	switch (mem->state) {
		case MEM_ONLINE:
			len = sprintf(buf, "online\n");
			break;
		case MEM_OFFLINE:
			len = sprintf(buf, "offline\n");
			break;
		case MEM_GOING_OFFLINE:
			len = sprintf(buf, "going-offline\n");
			break;
		default:
			len = sprintf(buf, "ERROR-UNKNOWN-%ld\n",
					mem->state);
			WARN_ON(1);
			break;
	}

	return len;
}

int memory_notify(unsigned long val, void *v)
{
	return blocking_notifier_call_chain(&memory_chain, val, v);
}

int memory_isolate_notify(unsigned long val, void *v)
{
	return atomic_notifier_call_chain(&memory_isolate_chain, val, v);
}

/*
 * The probe routines leave the pages reserved, just as the bootmem code does.
 * Make sure they're still that way.
 */
static bool pages_correctly_reserved(unsigned long start_pfn)
{
	int i, j;
	struct page *page;
	unsigned long pfn = start_pfn;

	/*
	 * memmap between sections is not contiguous except with
	 * SPARSEMEM_VMEMMAP. We lookup the page once per section
	 * and assume memmap is contiguous within each section
	 */
	for (i = 0; i < sections_per_block; i++, pfn += PAGES_PER_SECTION) {
		if (WARN_ON_ONCE(!pfn_valid(pfn)))
			return false;
		page = pfn_to_page(pfn);

		for (j = 0; j < PAGES_PER_SECTION; j++) {
			if (PageReserved(page + j))
				continue;

			printk(KERN_WARNING "section number %ld page number %d "
				"not reserved, was it already online?\n",
				pfn_to_section_nr(pfn), j);

			return false;
		}
	}

	return true;
}

/*
 * MEMORY_HOTPLUG depends on SPARSEMEM in mm/Kconfig, so it is
 * OK to have direct references to sparsemem variables in here.
 */
static int
memory_block_action(unsigned long phys_index, unsigned long action, int online_type)
{
	unsigned long start_pfn;
	unsigned long nr_pages = PAGES_PER_SECTION * sections_per_block;
	struct page *first_page;
	int ret;

	first_page = pfn_to_page(phys_index << PFN_SECTION_SHIFT);
	start_pfn = page_to_pfn(first_page);

	switch (action) {
		case MEM_ONLINE:
			if (!pages_correctly_reserved(start_pfn))
				return -EBUSY;

			ret = online_pages(start_pfn, nr_pages, online_type);
			break;
		case MEM_OFFLINE:
			ret = offline_pages(start_pfn, nr_pages);
			break;
		default:
			WARN(1, KERN_WARNING "%s(%ld, %ld) unknown action: "
			     "%ld\n", __func__, phys_index, action, action);
			ret = -EINVAL;
	}

	return ret;
}

static int __memory_block_change_state(struct memory_block *mem,
		unsigned long to_state, unsigned long from_state_req,
		int online_type)
{
	int ret = 0;

	if (mem->state != from_state_req)
		return -EINVAL;

	if (to_state == MEM_OFFLINE)
		mem->state = MEM_GOING_OFFLINE;

	ret = memory_block_action(mem->start_section_nr, to_state, online_type);
	mem->state = ret ? from_state_req : to_state;
	return ret;
}

static int memory_subsys_online(struct device *dev)
{
	struct memory_block *mem = container_of(dev, struct memory_block, dev);
	int ret;

	mutex_lock(&mem->state_mutex);

	ret = mem->state == MEM_ONLINE ? 0 :
		__memory_block_change_state(mem, MEM_ONLINE, MEM_OFFLINE,
					    ONLINE_KEEP);

	mutex_unlock(&mem->state_mutex);
	return ret;
}

static int memory_subsys_offline(struct device *dev)
{
	struct memory_block *mem = container_of(dev, struct memory_block, dev);
	int ret;

	mutex_lock(&mem->state_mutex);

	ret = mem->state == MEM_OFFLINE ? 0 :
		__memory_block_change_state(mem, MEM_OFFLINE, MEM_ONLINE, -1);

	mutex_unlock(&mem->state_mutex);
	return ret;
}

static int __memory_block_change_state_uevent(struct memory_block *mem,
		unsigned long to_state, unsigned long from_state_req,
		int online_type)
{
	int ret = __memory_block_change_state(mem, to_state, from_state_req,
					      online_type);
	if (!ret) {
		switch (mem->state) {
		case MEM_OFFLINE:
			kobject_uevent(&mem->dev.kobj, KOBJ_OFFLINE);
			break;
		case MEM_ONLINE:
			kobject_uevent(&mem->dev.kobj, KOBJ_ONLINE);
			break;
		default:
			break;
		}
	}
	return ret;
}

static int memory_block_change_state(struct memory_block *mem,
		unsigned long to_state, unsigned long from_state_req,
		int online_type)
{
	int ret;

	mutex_lock(&mem->state_mutex);
	ret = __memory_block_change_state_uevent(mem, to_state, from_state_req,
						 online_type);
	mutex_unlock(&mem->state_mutex);

	return ret;
}
static ssize_t
store_mem_state(struct device *dev,
		struct device_attribute *attr, const char *buf, size_t count)
{
	struct memory_block *mem;
	bool offline;
	int ret = -EINVAL;

	mem = container_of(dev, struct memory_block, dev);

	lock_device_hotplug();

	if (!strncmp(buf, "online_kernel", min_t(int, count, 13))) {
		offline = false;
		ret = memory_block_change_state(mem, MEM_ONLINE,
						MEM_OFFLINE, ONLINE_KERNEL);
	} else if (!strncmp(buf, "online_movable", min_t(int, count, 14))) {
		offline = false;
		ret = memory_block_change_state(mem, MEM_ONLINE,
						MEM_OFFLINE, ONLINE_MOVABLE);
	} else if (!strncmp(buf, "online", min_t(int, count, 6))) {
		offline = false;
		ret = memory_block_change_state(mem, MEM_ONLINE,
						MEM_OFFLINE, ONLINE_KEEP);
	} else if(!strncmp(buf, "offline", min_t(int, count, 7))) {
		offline = true;
		ret = memory_block_change_state(mem, MEM_OFFLINE,
						MEM_ONLINE, -1);
	}
	if (!ret)
		dev->offline = offline;

	unlock_device_hotplug();

	if (ret)
		return ret;
	return count;
}

/*
 * phys_device is a bad name for this.  What I really want
 * is a way to differentiate between memory ranges that
 * are part of physical devices that constitute
 * a complete removable unit or fru.
 * i.e. do these ranges belong to the same physical device,
 * s.t. if I offline all of these sections I can then
 * remove the physical device?
 */
static ssize_t show_phys_device(struct device *dev,
				struct device_attribute *attr, char *buf)
{
	struct memory_block *mem =
		container_of(dev, struct memory_block, dev);
	return sprintf(buf, "%d\n", mem->phys_device);
}

static DEVICE_ATTR(phys_index, 0444, show_mem_start_phys_index, NULL);
static DEVICE_ATTR(end_phys_index, 0444, show_mem_end_phys_index, NULL);
static DEVICE_ATTR(state, 0644, show_mem_state, store_mem_state);
static DEVICE_ATTR(phys_device, 0444, show_phys_device, NULL);
static DEVICE_ATTR(removable, 0444, show_mem_removable, NULL);

/*
 * Block size attribute stuff
 */
static ssize_t
print_block_size(struct device *dev, struct device_attribute *attr,
		 char *buf)
{
	return sprintf(buf, "%lx\n", get_memory_block_size());
}

static DEVICE_ATTR(block_size_bytes, 0444, print_block_size, NULL);

/*
 * Some architectures will have custom drivers to do this, and
 * will not need to do it from userspace.  The fake hot-add code
 * as well as ppc64 will do all of their discovery in userspace
 * and will require this interface.
 */
#ifdef CONFIG_ARCH_MEMORY_PROBE
static ssize_t
memory_probe_store(struct device *dev, struct device_attribute *attr,
		   const char *buf, size_t count)
{
	u64 phys_addr;
	int nid;
	int i, ret;
	unsigned long pages_per_block = PAGES_PER_SECTION * sections_per_block;

	phys_addr = simple_strtoull(buf, NULL, 0);

	if (phys_addr & ((pages_per_block << PAGE_SHIFT) - 1))
		return -EINVAL;

	for (i = 0; i < sections_per_block; i++) {
		nid = memory_add_physaddr_to_nid(phys_addr);
		ret = add_memory(nid, phys_addr,
				 PAGES_PER_SECTION << PAGE_SHIFT);
		if (ret)
			goto out;

		phys_addr += MIN_MEMORY_BLOCK_SIZE;
	}

	ret = count;
out:
	return ret;
}

static DEVICE_ATTR(probe, S_IWUSR, NULL, memory_probe_store);
#endif

#ifdef CONFIG_MEMORY_FAILURE
/*
 * Support for offlining pages of memory
 */

/* Soft offline a page */
static ssize_t
store_soft_offline_page(struct device *dev,
			struct device_attribute *attr,
			const char *buf, size_t count)
{
	int ret;
	u64 pfn;
	if (!capable(CAP_SYS_ADMIN))
		return -EPERM;
	if (strict_strtoull(buf, 0, &pfn) < 0)
		return -EINVAL;
	pfn >>= PAGE_SHIFT;
	if (!pfn_valid(pfn))
		return -ENXIO;
	ret = soft_offline_page(pfn_to_page(pfn), 0);
	return ret == 0 ? count : ret;
}

/* Forcibly offline a page, including killing processes. */
static ssize_t
store_hard_offline_page(struct device *dev,
			struct device_attribute *attr,
			const char *buf, size_t count)
{
	int ret;
	u64 pfn;
	if (!capable(CAP_SYS_ADMIN))
		return -EPERM;
	if (strict_strtoull(buf, 0, &pfn) < 0)
		return -EINVAL;
	pfn >>= PAGE_SHIFT;
	ret = memory_failure(pfn, 0, 0);
	return ret ? ret : count;
}

static DEVICE_ATTR(soft_offline_page, S_IWUSR, NULL, store_soft_offline_page);
static DEVICE_ATTR(hard_offline_page, S_IWUSR, NULL, store_hard_offline_page);
#endif

/*
 * Note that phys_device is optional.  It is here to allow for
 * differentiation between which *physical* devices each
 * section belongs to...
 */
int __weak arch_get_memory_phys_device(unsigned long start_pfn)
{
	return 0;
}

/*
 * A reference for the returned object is held and the reference for the
 * hinted object is released.
 */
struct memory_block *find_memory_block_hinted(struct mem_section *section,
					      struct memory_block *hint)
{
	int block_id = base_memory_block_id(__section_nr(section));
	struct device *hintdev = hint ? &hint->dev : NULL;
	struct device *dev;

	dev = subsys_find_device_by_id(&memory_subsys, block_id, hintdev);
	if (hint)
		put_device(&hint->dev);
	if (!dev)
		return NULL;
	return container_of(dev, struct memory_block, dev);
}

/*
 * For now, we have a linear search to go find the appropriate
 * memory_block corresponding to a particular phys_index. If
 * this gets to be a real problem, we can always use a radix
 * tree or something here.
 *
 * This could be made generic for all device subsystems.
 */
struct memory_block *find_memory_block(struct mem_section *section)
{
	return find_memory_block_hinted(section, NULL);
}

static struct attribute *memory_memblk_attrs[] = {
	&dev_attr_phys_index.attr,
	&dev_attr_end_phys_index.attr,
	&dev_attr_state.attr,
	&dev_attr_phys_device.attr,
	&dev_attr_removable.attr,
	NULL
};

static struct attribute_group memory_memblk_attr_group = {
	.attrs = memory_memblk_attrs,
};

static const struct attribute_group *memory_memblk_attr_groups[] = {
	&memory_memblk_attr_group,
	NULL,
};

/*
 * register_memory - Setup a sysfs device for a memory block
 */
static
int register_memory(struct memory_block *memory)
{
	int error;

	memory->dev.bus = &memory_subsys;
	memory->dev.id = memory->start_section_nr / sections_per_block;
	memory->dev.release = memory_block_release;
	memory->dev.groups = memory_memblk_attr_groups;
	memory->dev.offline = memory->state == MEM_OFFLINE;

	error = device_register(&memory->dev);
	return error;
}

static int init_memory_block(struct memory_block **memory,
			     struct mem_section *section, unsigned long state)
{
	struct memory_block *mem;
	unsigned long start_pfn;
	int scn_nr;
	int ret = 0;

	mem = kzalloc(sizeof(*mem), GFP_KERNEL);
	if (!mem)
		return -ENOMEM;

	scn_nr = __section_nr(section);
	mem->start_section_nr =
			base_memory_block_id(scn_nr) * sections_per_block;
	mem->end_section_nr = mem->start_section_nr + sections_per_block - 1;
	mem->state = state;
	mem->section_count++;
	mutex_init(&mem->state_mutex);
	start_pfn = section_nr_to_pfn(mem->start_section_nr);
	mem->phys_device = arch_get_memory_phys_device(start_pfn);

	ret = register_memory(mem);

	*memory = mem;
	return ret;
}

static int add_memory_section(int nid, struct mem_section *section,
			struct memory_block **mem_p,
			unsigned long state, enum mem_add_context context)
{
	struct memory_block *mem = NULL;
	int scn_nr = __section_nr(section);
	int ret = 0;

	mutex_lock(&mem_sysfs_mutex);

	if (context == BOOT) {
		/* same memory block ? */
		if (mem_p && *mem_p)
			if (scn_nr >= (*mem_p)->start_section_nr &&
			    scn_nr <= (*mem_p)->end_section_nr) {
				mem = *mem_p;
				kobject_get(&mem->dev.kobj);
			}
	} else
		mem = find_memory_block(section);

	if (mem) {
		mem->section_count++;
		kobject_put(&mem->dev.kobj);
	} else {
		ret = init_memory_block(&mem, section, state);
		/* store memory_block pointer for next loop */
		if (!ret && context == BOOT)
			if (mem_p)
				*mem_p = mem;
	}

	if (!ret) {
		if (context == HOTPLUG &&
		    mem->section_count == sections_per_block)
			ret = register_mem_sect_under_node(mem, nid);
	}

	mutex_unlock(&mem_sysfs_mutex);
	return ret;
}

/*
 * need an interface for the VM to add new memory regions,
 * but without onlining it.
 */
int register_new_memory(int nid, struct mem_section *section)
{
	return add_memory_section(nid, section, NULL, MEM_OFFLINE, HOTPLUG);
}

#ifdef CONFIG_MEMORY_HOTREMOVE
static void
unregister_memory(struct memory_block *memory)
{
	BUG_ON(memory->dev.bus != &memory_subsys);

	/* drop the ref. we got in remove_memory_block() */
	kobject_put(&memory->dev.kobj);
	device_unregister(&memory->dev);
}

static int remove_memory_block(unsigned long node_id,
			       struct mem_section *section, int phys_device)
{
	struct memory_block *mem;

	mutex_lock(&mem_sysfs_mutex);
	mem = find_memory_block(section);
	unregister_mem_sect_under_nodes(mem, __section_nr(section));

	mem->section_count--;
	if (mem->section_count == 0)
		unregister_memory(mem);
	else
		kobject_put(&mem->dev.kobj);

	mutex_unlock(&mem_sysfs_mutex);
	return 0;
}

int unregister_memory_section(struct mem_section *section)
{
	if (!present_section(section))
		return -EINVAL;

	return remove_memory_block(0, section, 0);
}
#endif /* CONFIG_MEMORY_HOTREMOVE */

/* return true if the memory block is offlined, otherwise, return false */
bool is_memblock_offlined(struct memory_block *mem)
{
	return mem->state == MEM_OFFLINE;
}

static struct attribute *memory_root_attrs[] = {
#ifdef CONFIG_ARCH_MEMORY_PROBE
	&dev_attr_probe.attr,
#endif

#ifdef CONFIG_MEMORY_FAILURE
	&dev_attr_soft_offline_page.attr,
	&dev_attr_hard_offline_page.attr,
#endif

	&dev_attr_block_size_bytes.attr,
	NULL
};

static struct attribute_group memory_root_attr_group = {
	.attrs = memory_root_attrs,
};

static const struct attribute_group *memory_root_attr_groups[] = {
	&memory_root_attr_group,
	NULL,
};

/*
 * Initialize the sysfs support for memory devices...
 */
int __init memory_dev_init(void)
{
	unsigned int i;
	int ret;
	int err;
	unsigned long block_sz;
	struct memory_block *mem = NULL;

	ret = subsys_system_register(&memory_subsys, memory_root_attr_groups);
	if (ret)
		goto out;

	block_sz = get_memory_block_size();
	sections_per_block = block_sz / MIN_MEMORY_BLOCK_SIZE;

	/*
	 * Create entries for memory sections that were found
	 * during boot and have been initialized
	 */
	for (i = 0; i < NR_MEM_SECTIONS; i++) {
		if (!present_section_nr(i))
			continue;
		/* don't need to reuse memory_block if only one per block */
		err = add_memory_section(0, __nr_to_section(i),
				 (sections_per_block == 1) ? NULL : &mem,
					 MEM_ONLINE,
					 BOOT);
		if (!ret)
			ret = err;
	}

out:
	if (ret)
		printk(KERN_ERR "%s() failed: %d\n", __func__, ret);
	return ret;
}
