#pragma once
#include <vector>
#include <ifs/ifs_types.h>

/////sys/ifs/ifs_types.h

// struct sbt_entry
// {
// 	btree_key_t key;
// 	struct btree_flags flags;
// 	uint32_t size_out;
// 	char buf[];
// };

// struct btree_key
// {
// 	uint64_t keys[2];  ////keys[0]保存djob_id
// };
// typedef struct btree_key btree_key_t;

/**
 * Defines a variable length array of sbt_entry pointers.
 */
typedef std::vector<struct sbt_entry *>
	sbt_entries;

/*
 * Filter function callback definition
 *
 * Returns true if SBT entry is acceptable, else false.  Iteration will
 * terminate on error.
 */
typedef bool (*sbt_filter_cb_t)(const struct sbt_entry *sbt_ent, void *cb_ctx,
								struct isi_error **error_out);

/**
 * This is a generic SBT reader. When pointed at an SBT, it provides tools
 * for the retrieval and manipulation of the contents. The class manages
 * all memory required to retrieve records from the SBT.
 *
 * -- Implementation
 * When get_first() is called, the class reads a collection of sbt_entries
 * from the beginning of the SBT and returns the first entry to the caller.
 * The get_next() call will return the next entry in the collection.
 * Eventually, get_next(), will have exhausted the collection. This will
 * prompt the class to read another collection from the SBT and get_next()
 * will then return the first entry in the new collection. This process will
 * continue till all the entries have been read from the SBT. get_next()
 * will return a NULL pointer when there are no more entries in the SBT.
 *
 *
 * 初始调用get_first(), 读取一些sbt_entries放进vector,并且返回第一个entry给get_first().
 * 之后调用get_next(),返回之前vector中的首个entry,最终当get_next()操作耗尽vector中的entry后，
 * get_next()将会调用get_first(),读取下一个block,用vector存放这些entries.
 * - At present, the class is not fully provisioned. More methods will be
 *   added over time.
 *
 * - Thread safety
 *   When used in a thread this should be safe. The contents of the object
 *   are not meant to be shared across threads. Changes in the SBT's contents
 *   will not reflect into the class's collection of entries. If this is used
 *   properly, it is safe.
 */
class sbt_reader
{
public:
	/**
	 * Constructor
	 * Used to instantiate the class.
	 */
	sbt_reader();

	/**
	 * Constructor
	 * Used to instantiate the class. Identifies the SBT to access.
	 *
	 * @param   int		Source SBT file descriptor
	 */
	sbt_reader(int fd);

	/**
	 * Destructor
	 * Releases all memory resources used by the class
	 */
	~sbt_reader();

	/**
	 * Alternate way to identify the source SBT. Changing the SBT will
	 * cause the next get_first/next() call to query the new SBT. The
	 * existing record pointer will remain valid till the next get_first()
	 * or get_next() call is issued.
	 *
	 * @param   int		Source SBT file descriptor
	 */
	void set_sbt(int fd);

	/*
	 * Set filter function
	 */
	void set_filter(sbt_filter_cb_t filter, void *filter_ctx);

	/**
	 * Retrieves the very first record in the SBT
	 *
	 * @param   struct isi_error**	Tracks retrieval issues
	 * @returns sbt_entry*		Pointer to the SBT record in the buffer
	 *				NULL if there isn't one
	 */
	const sbt_entry *get_first(struct isi_error **error_out);

	/**
	 * Retrieves the next entry record from the SBT
	 *
	 * @param   struct isi_error**	Tracks retrieval issues
	 * @returns sbt_entry*		Pointer to the SBT record in the buffer
	 *				NULL if there isn't one
	 */
	const sbt_entry *get_next(struct isi_error **error_out);

	size_t get_num(); //////返回vector中entries的数量

protected:
	/**
	 * Retrieves a collection of records from the SBT. The SBT is scanned
	 * for the specified key. The  buffer is populated with that
	 * record and as many of the consecutive records as the buffer can
	 * hold. The retrieved records can be accessed randomly through the
	 * returned vector of record pointers.
	 *
	 * - The list of records will be viable as long as this class remains
	 *   instantiated and until the next fetch.
	 *
	 * - When the SBT has been exhausted, the sbt_entries vector will
	 *   be empty
	 *
	 * @param   btree_key_t*	Target lookup key
	 * @param   btree_key_t*	Target for the next fetch
	 * @param   sbt_entries&	Vector of sbt_entry pointers
	 * @param   struct isi_error**	Tracks retrieval issues
	 */
	///////给定指定的一个btree key, 返回一个vector的sbt entries, 这些sbt entries随机保存
	void get_entries(
		btree_key_t *key, btree_key_t *next_key,
		sbt_entries &entries, struct isi_error **error_out);

	/**
	 * Clears the bookkeeping, returning it to a clean state.
	 *
	 * @returns void
	 */
	void reset();

	/**
	 * Allocates or clears the records buffer when a fetch is requested.
	 */
	void prep_buffer();

	static const int INVALID_FD; // Undefined file descriptor constant

	int m_fd; // Source SBT file descriptor  sbt树的fd

	//
	// A large buffer to collect entries into
	//
	char *m_buffer;			// Receives SBT entries  保存sbt entries的内容
	size_t m_buffer_length; // Buffer size
	sbt_entries m_entries;	// vector of buffer entries
	btree_key_t m_key;		// Tracks last fetch location
	int m_index;			// Current record

	sbt_filter_cb_t filter_cb;
	void *filter_cb_ctx;

private:
	/**
	 * Following functions are not implemented
	 */
	sbt_reader(const sbt_reader &src);
	sbt_reader &operator=(const sbt_reader &src);
};
