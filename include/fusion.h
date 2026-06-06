#ifndef FUSION_H
#define FUSION_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

uint32_t fusion_abi_version(void);
char *fusion_version_string(void);
char *fusion_parse_url_json(const char *input);
void fusion_string_free(char *ptr);

#ifdef __cplusplus
}
#endif

#endif
