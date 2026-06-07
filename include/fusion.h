#ifndef FUSION_H
#define FUSION_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define FUSION_ABI_VERSION 2

#define FUSION_LOGIC_API_VERSION 1

#define FUSION_OK 0
#define FUSION_ERR_INVALID_ARGUMENT -1
#define FUSION_ERR_RUNTIME -2
#define FUSION_ERR_NOT_RUNNING -3
#define FUSION_ERR_ALREADY_RUNNING -4

typedef struct FusionRuntime FusionRuntime;

uint32_t fusion_abi_version(void);
uint32_t fusion_logic_api_version(void);
char *fusion_version_string(void);

char *fusion_last_error(void);
void fusion_clear_last_error(void);

char *fusion_parse_url_json(const char *input);
char *fusion_validate_config_toml_json(const char *input);
char *fusion_filter_status_json(const char *snapshot_json, const char *scope);
void fusion_string_free(char *ptr);

FusionRuntime *fusion_runtime_create(void);
void fusion_runtime_destroy(FusionRuntime *handle);

int32_t fusion_runtime_load_config_file(FusionRuntime *handle, const char *path);
int32_t fusion_runtime_start(FusionRuntime *handle);
int32_t fusion_runtime_stop(FusionRuntime *handle);

char *fusion_runtime_status_json(FusionRuntime *handle, const char *scope);
char *fusion_runtime_task_request_json(FusionRuntime *handle, const char *request_json);

#ifdef __cplusplus
}
#endif

#endif
