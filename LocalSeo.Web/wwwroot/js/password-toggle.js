(function () {
    "use strict";

    function ensureWrapper(input) {
        var wrapper = input.closest(".password-toggle-wrap");
        if (wrapper) {
            return wrapper;
        }

        wrapper = document.createElement("div");
        wrapper.className = "password-toggle-wrap";
        input.parentNode.insertBefore(wrapper, input);
        wrapper.appendChild(input);
        return wrapper;
    }

    function ensureToggleButton(wrapper) {
        var button = wrapper.querySelector(".password-toggle-btn");
        if (!button) {
            button = document.createElement("button");
            button.type = "button";
            button.className = "password-toggle-btn";
            button.innerHTML = '<i class="bi bi-eye"></i>';
            wrapper.appendChild(button);
        }

        if (!button.hasAttribute("type")) {
            button.setAttribute("type", "button");
        }

        if (!button.querySelector("i")) {
            var icon = document.createElement("i");
            icon.className = "bi bi-eye";
            button.appendChild(icon);
        }

        return button;
    }

    function setMaskedState(input, button, masked) {
        input.type = masked ? "password" : "text";
        var icon = button.querySelector("i");
        if (!icon) {
            return;
        }

        icon.classList.remove("bi-eye", "bi-eye-slash");
        icon.classList.add(masked ? "bi-eye" : "bi-eye-slash");
    }

    function bindPasswordToggle(input) {
        if (!input || input.dataset.passwordToggleBound === "1") {
            return;
        }

        var wrapper = ensureWrapper(input);
        var button = ensureToggleButton(wrapper);

        setMaskedState(input, button, true);

        function toggle() {
            var isMasked = input.type === "password";
            setMaskedState(input, button, !isMasked);
        }

        button.addEventListener("click", toggle);
        button.addEventListener("keydown", function (event) {
            if (event.key === "Enter" || event.key === " ") {
                event.preventDefault();
                toggle();
            }
        });

        input.dataset.passwordToggleBound = "1";
    }

    document.addEventListener("DOMContentLoaded", function () {
        var passwordInputs = document.querySelectorAll("input.js-password-toggle");
        for (var i = 0; i < passwordInputs.length; i++) {
            bindPasswordToggle(passwordInputs[i]);
        }
    });
})();
