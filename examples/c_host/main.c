#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../include/fusion.h"

static void print_owned_string(const char *label, char *value) {
    if (value == NULL) {
        printf("%s=(null)\n", label);
        return;
    }
    printf("%s=%s\n", label, value);
    fusion_string_free(value);
}

int main(int argc, char **argv) {
    (void)argc;
    (void)argv;

    printf("fusion_abi_version=%u\n", fusion_abi_version());
    print_owned_string("fusion_version", fusion_version_string());

    char *parsed = fusion_parse_url_json("tcp://127.0.0.1:9000/task");
    print_owned_string("parse_url", parsed);

    if (argc > 1) {
        FusionRuntime *runtime = fusion_runtime_create();
        if (runtime == NULL) {
            print_owned_string("runtime_create_error", fusion_last_error());
            return 1;
        }

        int32_t load_code = fusion_runtime_load_config_file(runtime, argv[1]);
        printf("load_config_code=%d\n", load_code);
        if (load_code != FUSION_OK) {
            print_owned_string("load_config_error", fusion_last_error());
            fusion_runtime_destroy(runtime);
            return 1;
        }

        char *status = fusion_runtime_status_json(runtime, "routes");
        print_owned_string("status_routes", status);

        fusion_runtime_destroy(runtime);
    } else {
        printf("hint: pass path/to/fusion.toml to exercise runtime load/status\n");
    }

    return 0;
}
