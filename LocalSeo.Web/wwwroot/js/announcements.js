document.addEventListener("DOMContentLoaded", () => {
    const bellButton = document.getElementById("announcements-bell-button");
    const unreadBadge = document.getElementById("announcements-unread-badge");
    const overlay = document.getElementById("announcements-modal-overlay");
    const closeButton = document.getElementById("close-announcements-modal-button");
    const modeLabel = document.getElementById("announcements-modal-mode");
    const positionLabel = document.getElementById("announcements-modal-position");
    const emptyState = document.getElementById("announcements-modal-empty");
    const itemWrap = document.getElementById("announcements-modal-item");
    const titleNode = document.getElementById("announcements-modal-item-title");
    const metaNode = document.getElementById("announcements-modal-item-meta");
    const bodyNode = document.getElementById("announcements-modal-item-body");
    const prevButton = document.getElementById("announcements-modal-prev-button");
    const nextButton = document.getElementById("announcements-modal-next-button");
    const antiforgeryForm = document.getElementById("announcements-antiforgery-form");

    if (!(bellButton instanceof HTMLButtonElement)
        || !(unreadBadge instanceof HTMLElement)
        || !(overlay instanceof HTMLElement)
        || !(closeButton instanceof HTMLButtonElement)
        || !(modeLabel instanceof HTMLElement)
        || !(positionLabel instanceof HTMLElement)
        || !(emptyState instanceof HTMLElement)
        || !(itemWrap instanceof HTMLElement)
        || !(titleNode instanceof HTMLElement)
        || !(metaNode instanceof HTMLElement)
        || !(bodyNode instanceof HTMLElement)
        || !(prevButton instanceof HTMLButtonElement)
        || !(nextButton instanceof HTMLButtonElement)
        || !(antiforgeryForm instanceof HTMLFormElement)) {
        return;
    }

    let mode = "latest";
    let items = [];
    let index = 0;
    let isOpen = false;

    const getAntiForgeryToken = () => {
        const tokenInput = antiforgeryForm.querySelector("input[name='__RequestVerificationToken']");
        return tokenInput instanceof HTMLInputElement ? tokenInput.value : "";
    };

    const setUnreadBadge = (count) => {
        const safeCount = Number.isFinite(count) && count > 0 ? Math.floor(count) : 0;
        bellButton.dataset.announcementsUnreadCount = String(safeCount);
        bellButton.classList.toggle("has-unread", safeCount > 0);
        unreadBadge.textContent = String(safeCount);
        unreadBadge.classList.toggle("is-hidden", safeCount <= 0);
    };

    const setModeLabel = () => {
        modeLabel.textContent = mode === "unread"
            ? "Showing unread announcements"
            : "Showing latest announcements";
    };

    const setNavState = () => {
        const hasItems = items.length > 0;
        prevButton.disabled = !hasItems || index <= 0;
        nextButton.disabled = !hasItems || index >= items.length - 1;
        positionLabel.textContent = hasItems ? `${index + 1} of ${items.length}` : "";
    };

    const formatUtc = (utcValue) => {
        if (!utcValue)
            return "";

        const date = new Date(utcValue);
        if (Number.isNaN(date.getTime()))
            return "";

        return date.toLocaleString();
    };

    const renderCurrent = async () => {
        setModeLabel();

        if (items.length === 0) {
            emptyState.classList.remove("is-hidden");
            itemWrap.classList.add("is-hidden");
            setNavState();
            return;
        }

        const item = items[index];
        emptyState.classList.add("is-hidden");
        itemWrap.classList.remove("is-hidden");

        titleNode.textContent = item.title || "Announcement";
        const createdAtText = formatUtc(item.createdAtUtc);
        const byName = (item.createdByName || "").trim();
        if (createdAtText && byName) {
            metaNode.textContent = `${createdAtText} by ${byName}`;
        } else if (createdAtText) {
            metaNode.textContent = createdAtText;
        } else if (byName) {
            metaNode.textContent = `By ${byName}`;
        } else {
            metaNode.textContent = "";
        }

        bodyNode.innerHTML = item.htmlBody || "";
        setNavState();
        await markRead(item.id);
    };

    const loadFeed = async () => {
        modeLabel.textContent = "Loading...";
        positionLabel.textContent = "";
        items = [];
        index = 0;
        setNavState();

        const response = await fetch("/announcements/modal-feed", {
            method: "GET",
            headers: { "Accept": "application/json" }
        });

        if (!response.ok) {
            throw new Error(`Failed to load announcements (${response.status}).`);
        }

        const data = await response.json();
        mode = data && typeof data.mode === "string" ? data.mode : "latest";
        items = Array.isArray(data?.items) ? data.items : [];
    };

    const markRead = async (announcementId) => {
        const id = Number(announcementId || 0);
        if (id <= 0) {
            return;
        }

        const token = getAntiForgeryToken();
        if (!token) {
            return;
        }

        try {
            const response = await fetch("/announcements/mark-read", {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "RequestVerificationToken": token
                },
                body: JSON.stringify({ announcementId: id })
            });

            if (!response.ok) {
                return;
            }

            const data = await response.json();
            setUnreadBadge(Number(data?.count || 0));
        } catch {
            // Keep modal interaction working even if mark-read fails.
        }
    };

    const openModal = async () => {
        if (isOpen)
            return;

        isOpen = true;
        overlay.classList.remove("is-hidden");
        overlay.setAttribute("aria-hidden", "false");
        document.body.classList.add("modal-open");

        try {
            await loadFeed();
            if (!isOpen)
                return;
            await renderCurrent();
        } catch (error) {
            emptyState.classList.remove("is-hidden");
            itemWrap.classList.add("is-hidden");
            mode = "latest";
            modeLabel.textContent = "Unable to load announcements.";
            positionLabel.textContent = "";
            setNavState();
            if (window.console && typeof window.console.error === "function") {
                window.console.error(error);
            }
        }
    };

    const closeModal = () => {
        if (!isOpen)
            return;

        isOpen = false;
        overlay.classList.add("is-hidden");
        overlay.setAttribute("aria-hidden", "true");
        document.body.classList.remove("modal-open");
        bellButton.focus();
    };

    prevButton.addEventListener("click", async () => {
        if (index <= 0)
            return;
        index -= 1;
        await renderCurrent();
    });

    nextButton.addEventListener("click", async () => {
        if (index >= items.length - 1)
            return;
        index += 1;
        await renderCurrent();
    });

    bellButton.addEventListener("click", openModal);
    closeButton.addEventListener("click", closeModal);
    overlay.addEventListener("click", (event) => {
        if (event.target === overlay) {
            closeModal();
        }
    });

    document.addEventListener("keydown", (event) => {
        if (event.key === "Escape" && isOpen) {
            closeModal();
        }
    });

    const initialUnreadCount = Number(bellButton.dataset.announcementsUnreadCount || "0");
    setUnreadBadge(initialUnreadCount);
});
