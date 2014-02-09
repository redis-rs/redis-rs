RUSTC?=rustc
RUSTFLAGS?=
OUTPUT_PATH?=build

CRATE_LIB_SRC=src/redis/lib.rs
CRATE_LIB_FILENAME=$(shell $(RUSTC) --crate-file-name $(CRATE_LIB_SRC))
CRATE_LIB_DEPFILE=$(OUTPUT_PATH)/.$(CRATE_LIB_FILENAME).deps.mk
CRATE_LIB=$(OUTPUT_PATH)/$(CRATE_LIB_FILENAME)

CRATE_TEST_SRC=src/redis/test.rs
CRATE_TEST_DEPFILE=$(OUTPUT_PATH)/.redis-test.deps.mk
CRATE_TEST=$(OUTPUT_PATH)/redis-test

# Convenience functions
all: compile

compile: $(CRATE_LIB)

test: $(CRATE_TEST)
	@RUST_TEST_TASKS=1 ./$(CRATE_TEST)

clean:
	@rm -f $(CRATE_LIB_DEPFILE)
	@rm -f $(CRATE_TEST_DEPFILE)
	@rm -f $(CRATE_LIB)
	@rm -f $(CRATE_TEST)

.PHONY: all compile test clean

# Build steps
$(CRATE_LIB): $(CRATE_LIB_SRC)
	@mkdir -p $(OUTPUT_PATH)
	@$(RUSTC) $(RUSTFLAGS) \
		--dep-info=$(CRATE_LIB_DEPFILE) \
		-o $(CRATE_LIB) \
		$(CRATE_LIB_SRC)

$(CRATE_TEST): $(CRATE_TEST_SRC) $(CRATE_LIB)
	@mkdir -p $(OUTPUT_PATH)
	@$(RUSTC) $(RUSTFLAGS) \
		--dep-info=$(CRATE_TEST_DEPFILE) \
		-o $(CRATE_TEST) \
		-L $(OUTPUT_PATH) \
		--test $(CRATE_TEST_SRC)

# Dependencies
-include $(CRATE_LIB_DEPFILE)
-include $(CRATE_TEST_DEPFILE)
