
#include <ifs/ifs_constants.h>
#include <ifs/ifs_syscalls.h>
#include <isi_util/isi_error.h>
#include <isi_ilog/ilog.h>

#include <isi_cpool_d_common/sbt_reader.h>

const int sbt_reader::INVALID_FD = -1;

static const int SUCCESS = 0;

sbt_reader::sbt_reader()
	: m_fd(INVALID_FD), m_buffer(NULL), m_buffer_length(IFS_BSIZE) /*8192:8M*/, m_index(-1), filter_cb(NULL), filter_cb_ctx(NULL)
{
}

sbt_reader::sbt_reader(int fd)
	: m_fd(fd), m_buffer(NULL), m_buffer_length(IFS_BSIZE /*8192:8M*/), m_index(-1), filter_cb(NULL), filter_cb_ctx(NULL)
{
}

void sbt_reader::set_sbt(int fd)
{
	/**
	 * Only change the descriptor if it is different than the current
	 */
	if (fd != m_fd)
	{
		/**
		 * Currently this class does not actually open the SBT, so for now
		 * it will not close it when switching to another.
		 * - Reset the bookkeeping so that future first/next will
		 *   be directed to the new SBT
		 */
		m_fd = fd;

		reset();
	}
}

void sbt_reader::set_filter(sbt_filter_cb_t filter, void *filter_ctx)
{
	filter_cb = filter;
	filter_cb_ctx = filter_ctx;
}

size_t sbt_reader::get_num() ////for test only
{
	return m_entries.size();
}

/////拿一个vector的sbt entries
void sbt_reader::get_entries(
	btree_key_t *key /*当前想要查询的key*/, btree_key_t *next_key /*下一次fetch,想要查询的key*/,
	sbt_entries &entries, struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	// Can only retrieve what the buffer can hold
	int desired = m_buffer_length / sizeof(struct sbt_entry);
	size_t num_entries_out = 0;

	/**
	 * Each time the file is read from,  the contents of the previous
	 * read is destroyed.
	 */
	entries.clear();
	prep_buffer();

	int rc = SUCCESS;

	/**
	 * The most that can be retrieved is limited by the size of the
	 * buffer. Retrieve the desired amount or as much of it as possible.
	 * Retrieving less than requested does not imply the the entire SBT
	 * was collected.
	 */
	rc = ifs_sbt_get_entries(
		m_fd, key, next_key, m_buffer_length, m_buffer, desired,
		&num_entries_out);
	ilog(IL_INFO, "buf length:%lu entry size:%lu num_entries_put:%lu desired:%d", m_buffer_length, sizeof(struct sbt_entry),
		 num_entries_out, desired);
	/**
	 * Create a vector with the addresses of each entry. This will speed
	 * access by the caller and eliminate confusion with calculating the
	 * next entry.
	 */
	if (SUCCESS == rc)
	{
		char *buffer = m_buffer;

		for (int i = 0; i < num_entries_out; i++)
		{
			sbt_entry *entry = (sbt_entry *)buffer;
			entries.push_back(entry);

			buffer = &(entry->buf[0]) + entry->size_out;
		}
	}
	else
	{
		/**
		 * Create an error based upon errno
		 */
		error = isi_system_error_new(errno,
									 "Unable to retrieve the next batch "
									 "of SBT records. Key = %zu/%zu",
									 key->keys[0], key->keys[1]);
	}

	isi_error_handle(error, error_out);
}

/////get_first()始终拿的是当前这个vector中的第0个元素,因为reset(),m_index = 0
const sbt_entry *sbt_reader::get_first(struct isi_error **error_out)
{
	struct isi_error *error = NULL;

	reset();

	const sbt_entry *pentry = get_next(&error);

	isi_error_handle(error, error_out);
	return pentry;
}

void sbt_reader::reset()
{
	/**
	 * Clear all bookkeeping so that the SBT is accessed from the
	 * beginning.
	 */
	m_key.keys[0] = 0;
	m_key.keys[1] = 0;
	m_index = -1;
	m_entries.clear();
}

const sbt_entry *sbt_reader::get_next(struct isi_error **error_out)
{
	struct isi_error *error = NULL;
	sbt_entry *pentry = NULL;

	while (pentry == NULL)
	{
		m_index++; // Advance to next potential entry

		/**
		 * If the buffer is exhausted, get more
		 */
		if (m_index >= m_entries.size())
		{
			get_entries(&m_key, &m_key, m_entries, &error);
			ON_ISI_ERROR_GOTO(out, error);

			m_index = 0;
		}

		/* If we don't have an entry to consider, bail out. */
		if (m_entries.size() > 0)
		{
			pentry = m_entries[m_index];
		}
		else
		{
			break;
		}

		/*
		 * If there's a filter function, and it filters out this
		 * entry, try again.
		 */
		if (filter_cb != NULL)
		{
			bool matches_filter = filter_cb(pentry, filter_cb_ctx,
											&error);
			ON_ISI_ERROR_GOTO(out, error,
							  "filter callback error (key: %{})",
							  btree_key_fmt(&pentry->key));

			if (!matches_filter)
				pentry = NULL;
		}
	}

out:
	isi_error_handle(error, error_out);
	return pentry;
}

sbt_reader::~sbt_reader()
{
	delete[] m_buffer;
	m_buffer = NULL;
}

void sbt_reader::prep_buffer()
{
	if (NULL == m_buffer)
	{
		m_buffer = new char[m_buffer_length];
	}
	else
	{
		::memset(m_buffer, 0, m_buffer_length);
	}
}
