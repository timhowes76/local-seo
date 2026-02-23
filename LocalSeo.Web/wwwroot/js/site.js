document.addEventListener("DOMContentLoaded", () => {
    if (typeof TomSelect === "undefined") {
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
});
