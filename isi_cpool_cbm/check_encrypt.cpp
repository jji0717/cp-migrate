
#include <check.h>
#include <stdio.h>
#include <isi_cpool_security/cpool_protect.h>
#include "io_helper/encrypt_ctx.h"
#include "io_helper/encrypt_filter.h"
#include "io_helper/encrypted_istream.h"
#include "isi_cbm_data_stream.h"
#include "isi_cbm_test_util.h"
#include "isi_cbm_stream.h"

TEST_FIXTURE(suite_setup)
{
	struct isi_error *error = NULL;

	cpool_regenerate_mek(&error);

	fail_if(error, "failed to generate mek %#{}", isi_error_fmt(error));

	cpool_generate_dek(&error);

	fail_if(error, "failed to generate dek %#{}", isi_error_fmt(error));
}

TEST_FIXTURE(suite_teardown)
{
}

SUITE_DEFINE_FOR_FILE(check_encryption,
    .mem_check = CK_MEM_LEAKS,
    .suite_setup = suite_setup,
    .suite_teardown = suite_teardown,
    .timeout = 60,
    .fixture_timeout = 1200);

TEST(test_encryption, mem_check: CK_MEM_DEFAULT, mem_hint: 0)
{
	struct isi_error *error = NULL;
	encrypt_ctx enc_ctx;
	encrypt_ctx dec_ctx;
	const int length = 1024 * 1024;
	const int readsz = 128 * 1024;
	const int strmsz = 8 * 1024;
	char *encrypted_txt = (char *) malloc(length);
	const char *test_file = "/ifs/data/encryption_test.txt";
	const char *test_file_dec = "/ifs/data/encryption_test_dec.txt";

	cbm_test_file_helper fhelper(test_file, length, "");
	int fd = open(test_file, O_RDONLY);

	fail_if(fd < 0, "failed to open test file");

	// initialize encryption/decryption parameters
	enc_ctx.initialize(&error);
	fail_if(error, "failed to initialize encryption context %#{}",
	    isi_error_fmt(error));
	enc_ctx.chunk_idx = 1;

	dec_ctx.initialize(enc_ctx.mek_id, enc_ctx.edek,
	    enc_ctx.master_iv, &error);
	fail_if(error, "failed to initialize decryption context %#{}",
	    isi_error_fmt(error));
	dec_ctx.chunk_idx = 1;

	// encrypt data
	data_istream istrm(fd);
	encrypted_istream e_istrm(&enc_ctx, istrm, length, readsz);

	// first read, header takes 128 + 16 bytes
	fail_if(e_istrm.read(encrypted_txt, strmsz - (128 + 16)) !=
	    strmsz - (128 + 16));

	for (int i = strmsz - (128 + 16); i < length; i += strmsz) {
		int bytes = MIN(length - i, strmsz);

		fail_if(e_istrm.read(encrypted_txt + i, bytes) !=
		    (size_t)bytes);
	}
	e_istrm.reset();
	close(fd);

	// decrypt data
	fd = open(test_file_dec, O_CREAT | O_RDWR | O_TRUNC, 0644);
	fail_if(fd < 0, "failed to open test file: %s", test_file_dec);

	data_ostream ostrm(fd);
	encrypt_filter encrypt_f(ostrm, &dec_ctx, readsz);

	for (int i = 0; i < length; i += strmsz) {
		int bytes = MIN(length - i, strmsz);

		fail_if(encrypt_f.write(encrypted_txt + i, bytes) !=
		    (size_t)bytes);
	}
	encrypt_f.flush();

	fail_if(file_compare(test_file, test_file_dec), "encryption/decryption error");

	unlink(test_file);
	unlink(test_file_dec);
	free(encrypted_txt);
}
