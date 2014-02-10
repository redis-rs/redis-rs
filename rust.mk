# The rust compiler to use
RUSTC?=rustc

# The flags to pass to the rust compiler
RUSTFLAGS?=

# The output directory to place the binaries in
RUST_OUT_DIR?=build

# Rust crate helper
#
# Usage: $(eval $(call RUST_CRATE,<variable_name>,<source_path>,<flags>))
#
# flags can be left empty.  Whitespace screws things up.
# The following variables are generated:
#
#   <variable_name>_OUT          output filename for crate
#   <variable_name>_DEPFILE      the dependency file
#   <variable_name>_FILENAME     the filename without path
#   <variable_name>_CLEAN        variable pointing to a clean target
#                                that removes build data + depfile
#
define RUST_CRATE

$(1)_FILENAME=$$(shell $(RUSTC) $(RUSTFLAGS) --crate-file-name $(3) $(2))
$(1)_DEPFILE=$$(RUST_OUT_DIR)/.$$($(1)_FILENAME).deps.mk
$(1)_OUT=$$(RUST_OUT_DIR)/$$($(1)_FILENAME)
$(1)_CLEAN=__rust_clean_$(1)_CLEAN

$$($(1)_OUT): $(2) $$(shell which $(RUSTC))
	@echo "Compiling $$< -> $$@"
	@mkdir -p $$(RUST_OUT_DIR)
	@$$(RUSTC) $$(RUSTFLAGS) \
		--dep-info=$$($(1)_DEPFILE) \
		-o $$($(1)_OUT) \
		-L $$(RUST_OUT_DIR) \
		$(3) $(2)

.PHONY: __rust_clean_$(1)_CLEAN
__rust_clean_$(1)_CLEAN:
	@rm -f $$($(1)_OUT)
	@rm -f $$($(1)_DEPFILE)

-include $$($(1)_DEPFILE)

endef
