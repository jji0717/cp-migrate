
#ifndef __SPARSE_AREA_HEADER_H__
#define __SPARSE_AREA_HEADER_H__

#include <stdio.h>
#include <ifs/ifs_types.h>
#include <isi_ilog/ilog.h>
#include <isi_cloud_common/isi_cpool_version.h>

#define V1_SPARSE_MAP_SIZE (32)
#define SPARSE_MAP_SIZE V1_SPARSE_MAP_SIZE

#define ROUND_UP_SPA_RESOLUTION(spa_res)\
	((spa_res) <= 0) ? \
	    0 : \
	    ((((spa_res) + IFS_BSIZE - 1) / IFS_BSIZE) * IFS_BSIZE)

#define ROUND_DWN_SPA_RESOLUTION(spa_res)\
	( ((spa_res) > IFS_BSIZE) ?  \
	    (((spa_res) / IFS_BSIZE) * IFS_BSIZE) : \
	    (spa_res))

#define REQUIRED_SPA_MAP_BITS(chnk, spa_res) \
	(((chnk) + (spa_res) - 1) / (spa_res))

#define REQUIRED_SPA_MAP_BYTES(chnk, spa_res)\
	((REQUIRED_SPA_MAP_BITS(chnk, (spa_res)) + 7) / 8)

#define MAX_SPA_CHUNK_SIZE(spa_res)\
	(ROUND_UP_SPA_RESOLUTION(spa_res) * SPARSE_MAP_SIZE * 8)

#define MIN_SPA_RESOLUTION(chnk)\
	(((chnk) / (SPARSE_MAP_SIZE * 8)) < IFS_BSIZE) ? \
	    (IFS_BSIZE) : \
	    (ROUND_UP_SPA_RESOLUTION((chnk) + (SPARSE_MAP_SIZE * 8) - 1) / \
	    (SPARSE_MAP_SIZE * 8))

#define MAX_SPA_RESOLUTION(chnk)\
	(chnk)


typedef struct spa_header {
	friend class isi_cpool_master_cdo;
	uint16_t spa_version;
	uint16_t sparse_map_length;
	uint32_t sparse_resolution;  // Size represented by bit in sparse map
	uint8_t  sparse_map[SPARSE_MAP_SIZE];

public:

	spa_header(void) : spa_version(CPOOL_SPA_VERSION),
		sparse_map_length(0), sparse_resolution(0) {
		memset(sparse_map, 0, SPARSE_MAP_SIZE);
	}

	spa_header(uint32_t resolution) : spa_version(CPOOL_SPA_VERSION),
		sparse_map_length(0), sparse_resolution(resolution) {
		memset(sparse_map, 0, SPARSE_MAP_SIZE);
	}

	void trace_spa(
	    const char *msg, const char *fnct, const int lne) const {
		ILOG_START(IL_DEBUG) {
			int indx;
			fmt_print(ilogfmt,
			    "%s, Called from %s/%d, sparse header (%p): "
			    "version %d, "
			    "map length %d, "
			    "resolution %d, "
			    "map ",
			    msg, fnct, lne, this,
			    spa_version, sparse_map_length,
			    sparse_resolution
			    );
			for (indx = 0;
			     indx < MIN(SPARSE_MAP_SIZE, sparse_map_length);
			     indx++) {
				fmt_print(ilogfmt, "%02x ", sparse_map[indx]);
			}
		} ILOG_END;
	}

	void dump_spa(const char *name, int index) {
		int i;
		struct fmt FMT_INIT_CLEAN(spafmt);

		fmt_print(&spafmt,
		    "For file %s, CDO index %d: "
		    "version %d, "
		    "map length %d, "
		    "resolution %d, "
		    "map ",
		    name, index, spa_version, sparse_map_length,
		    sparse_resolution);

		for (i = 0; i < MIN(SPARSE_MAP_SIZE, sparse_map_length); i++) {
			fmt_print(&spafmt, "%02x ", sparse_map[i]);
		}
		printf("%s\n", fmt_string(&spafmt));
	}

	uint16_t get_spa_version() const {
		return spa_version;
	}
	uint16_t get_sparse_map_length() const {
		return sparse_map_length;
	}
	uint32_t get_sparse_resolution() const {
		return sparse_resolution;
	}
	const uint8_t *get_sparse_map_ptr() const {
		return &sparse_map[0];
	}
	uint8_t get_sparse_map_elem(int indx) {
		return sparse_map[indx];
	}

	void set_spa_version(uint16_t vers) {
		spa_version = vers;
	}
	void set_sparse_map_length(uint16_t len) {
		sparse_map_length = len;
	}
	void set_sparse_resolution(uint32_t res, size_t chnk) {
		sparse_resolution = MIN(ROUND_UP_SPA_RESOLUTION(res),
		    MAX_SPA_RESOLUTION(chnk));
	}
	void set_sparse_map(const uint8_t *smap, int len) {
		if (len < SPARSE_MAP_SIZE) {
			memset(sparse_map, 0, SPARSE_MAP_SIZE);
		}
		memcpy(sparse_map, smap, MIN(len, SPARSE_MAP_SIZE));
	}
	void set_sparse_map_elem(int indx, uint8_t val) {
		sparse_map[indx] = val;
	}
	void clear_sparse_map() {
		memset(sparse_map, 0, SPARSE_MAP_SIZE);
	}
} sparse_area_header_t;


#endif

