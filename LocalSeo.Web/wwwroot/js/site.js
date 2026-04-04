document.addEventListener("DOMContentLoaded", () => {
    if (typeof TomSelect === "undefined") {
        initializeSlugGenerators();
        return;
    }

    document.querySelectorAll("select.js-searchable").forEach((el) => {
        if (el.tomselect) {
            return;
        }

        new TomSelect(el, {
            create: false,
            allowEmptyOption: true,
            closeAfterSelect: true,
            maxOptions: 5000,
            hideSelected: false,
            placeholder: el.dataset.placeholder || "Search",
            plugins: ["dropdown_input"]
        });
    });

    initializeSlugGenerators();
});

function initializeSlugGenerators() {
    document.querySelectorAll("[data-slug-generator]").forEach((button) => {
        if (!(button instanceof HTMLButtonElement)) {
            return;
        }

        const source = document.getElementById(button.dataset.slugSource || "");
        const target = document.getElementById(button.dataset.slugTarget || "");
        if (!(source instanceof HTMLInputElement) || !(target instanceof HTMLInputElement)) {
            return;
        }

        const clearValidation = () => target.setCustomValidity("");
        source.addEventListener("input", clearValidation);
        target.addEventListener("input", clearValidation);

        button.addEventListener("click", () => {
            const result = generateSlugValue(source.value);
            if (!result.success) {
                target.value = "";
                target.setCustomValidity(result.error);
                target.reportValidity();
                return;
            }

            target.value = result.value;
            target.setCustomValidity("");
            target.dispatchEvent(new Event("input", { bubbles: true }));
        });
    });
}

function generateSlugValue(value) {
    const trimmed = (value || "").trim();
    if (!trimmed) {
        return { success: false, value: "", error: "Name is required to generate a slug." };
    }

    const slug = buildSlug(trimmed);
    if (!slug) {
        return { success: false, value: "", error: "Slug must contain at least one letter or number." };
    }

    if (slug === "." || slug === ".." || trimmed === "." || trimmed === "..") {
        return { success: false, value: "", error: "Slug cannot be '.' or '..'." };
    }

    return { success: true, value: slug, error: "" };
}

function buildSlug(value) {
    const transliterated = (value || "")
        .replace(/&/g, " and ")
        .replace(/['"\u0060\u2019\u2018\u201A\u201B\u201C\u201D\u201E\u201F]/g, "")
        .replace(/\u00DF/g, "ss")
        .replace(/\u00C6/g, "AE")
        .replace(/\u00E6/g, "ae")
        .replace(/\u0152/g, "OE")
        .replace(/\u0153/g, "oe")
        .replace(/\u00D8/g, "O")
        .replace(/\u00F8/g, "o")
        .replace(/\u00D0/g, "D")
        .replace(/\u00F0/g, "d")
        .replace(/\u00DE/g, "TH")
        .replace(/\u00FE/g, "th")
        .replace(/\u0141/g, "L")
        .replace(/\u0142/g, "l")
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .toLowerCase();

    return transliterated
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/-+/g, "-")
        .replace(/^-+|-+$/g, "");
}
