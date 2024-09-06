#include "cpool_d_debug.h"

void
print_buffer_as_hex(const void *buffer, size_t buffer_size, struct fmt &fmt)
{
	const unsigned char *c = static_cast<const unsigned char *>(buffer);
	for (size_t i = 0; i < buffer_size; ++i) {
		if (i % 32 == 0)
			fmt_print(&fmt, "\n");
		else if (i % 8 == 0)
			fmt_print(&fmt, " ");

		fmt_print(&fmt, "%02x", c[i]);
	}
}

void
print_hsbt_bulk_ent(const struct hsbt_bulk_entry &hsbt_ent, struct fmt &fmt)
{
	fmt_print(&fmt, "is_hash_btree? %s\n",
	    hsbt_ent.is_hash_btree ? "yes" : "no");
	struct fmt FMT_INIT_CLEAN(key_fmt);
	print_buffer_as_hex(hsbt_ent.key, hsbt_ent.key_len, key_fmt);
	fmt_print(&fmt, "hsbt_ent.key: %s\n", fmt_string(&key_fmt));
	fmt_print(&fmt, "hsbt_ent.key_length: %zu\n", hsbt_ent.key_len);

	print_sbt_bulk_ent(hsbt_ent.bulk_entry, fmt);
}

void
print_sbt_bulk_ent(const struct sbt_bulk_entry &sbt_ent, struct fmt &fmt)
{
	fmt_print(&fmt, "fd: %d\n", sbt_ent.fd);
	fmt_print(&fmt, "op_type: %d\n", sbt_ent.op_type);
	fmt_print(&fmt, "key: %lu/%lu\n", sbt_ent.key.keys[0],
	    sbt_ent.key.keys[1]);
	struct fmt FMT_INIT_CLEAN(entry_buf_fmt);
	print_buffer_as_hex(sbt_ent.entry_buf, sbt_ent.entry_size,
	    entry_buf_fmt);
	fmt_print(&fmt, "entry_buf: %s\n", fmt_string(&entry_buf_fmt));
	fmt_print(&fmt, "entry_size: %d\n", sbt_ent.entry_size);
	fmt_print(&fmt, "cm: %d\n", sbt_ent.cm);
	struct fmt FMT_INIT_CLEAN(old_entry_buf_fmt);
	print_buffer_as_hex(sbt_ent.old_entry_buf, sbt_ent.old_entry_size,
	    old_entry_buf_fmt);
	fmt_print(&fmt, "old_entry_buf: %s\n", fmt_string(&old_entry_buf_fmt));
	fmt_print(&fmt, "old_entry_size: %d\n", sbt_ent.old_entry_size);
}
