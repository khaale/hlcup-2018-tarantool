local ffi = require('ffi')
ffi.cdef[[
    typedef struct roaring_array_s {
        int32_t size;
        int32_t allocation_size;
        void **containers;
        uint16_t *keys;
        uint8_t *typecodes;
    } roaring_array_t;

    typedef struct roaring_bitmap_s {
        roaring_array_t high_low_container;
        bool copy_on_write;
    } roaring_bitmap_t;
    /**
     * Creates a new bitmap (initially empty)
     */
    roaring_bitmap_t *roaring_bitmap_create(void);
    /**
    * Creates a new bitmap (initially empty) with a provided
    * container-storage capacity (it is a performance hint).
    */
    roaring_bitmap_t *roaring_bitmap_create_with_capacity(uint32_t cap);

    /**
    * Add value x
    *
    */
    void roaring_bitmap_add(roaring_bitmap_t *r, uint32_t x);

    /**
    * Describe the inner structure of the bitmap.
    */
    void roaring_bitmap_printf_describe(const roaring_bitmap_t *ra);

    /**
    * Computes the size of the intersection between two bitmaps.
    *
    */
    uint64_t roaring_bitmap_and_cardinality(const roaring_bitmap_t *x1, const roaring_bitmap_t *x2);
]]
local roaringlib = ffi.load(ffi.os == "Windows" and "roaring" or "libroaring")

return {
    create_bitmap = function () return roaringlib.roaring_bitmap_create() end,
    create_bitmap_with_capacity = function (capacity) return roaringlib.roaring_bitmap_create_with_capacity(capacity) end,
    describe_bitmap = function (bitmap) return roaringlib.roaring_bitmap_printf_describe(bitmap) end,
    add_to_bitmap = function (bitmap, value)
        return roaringlib.roaring_bitmap_add(bitmap, ffi.new("uint32_t", value))
    end,
    and_cardinality = function (bmp1, bmp2)
        return tonumber(roaringlib.roaring_bitmap_and_cardinality(bmp1, bmp2))
    end
}
