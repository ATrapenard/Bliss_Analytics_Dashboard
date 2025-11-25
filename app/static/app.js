// Global App JS
document.addEventListener("DOMContentLoaded", () => {
  // --- Hamburger Menu Logic ---
  const hamburger = document.querySelector(".hamburger");
  const navLinks = document.querySelector(".nav-links");
  if (hamburger) {
    hamburger.addEventListener("click", () => {
      navLinks.classList.toggle("active");
      document
        .querySelectorAll(".dropdown .dropbtn.active")
        .forEach((button) => {
          button.classList.remove("active");
          let content = button.nextElementSibling;
          if (content && content.classList.contains("dropdown-content")) {
            content.style.display = "none";
          }
        });
    });
  }

  // --- Dropdown Click Logic (for mobile) ---
  document.querySelectorAll(".dropdown .dropbtn").forEach((button) => {
    button.addEventListener("click", (event) => {
      if (window.getComputedStyle(hamburger).display !== "none") {
        event.preventDefault();
        document
          .querySelectorAll(".dropdown .dropbtn.active")
          .forEach((otherButton) => {
            if (otherButton !== button) {
              otherButton.classList.remove("active");
              let otherContent = otherButton.nextElementSibling;
              if (
                otherContent &&
                otherContent.classList.contains("dropdown-content")
              ) {
                otherContent.style.display = "none";
              }
            }
          });
        button.classList.toggle("active");
        let content = button.nextElementSibling;
        if (content && content.classList.contains("dropdown-content")) {
          if (content.style.display === "block") {
            content.style.display = "none";
          } else {
            content.style.display = "block";
          }
        }
      }
    });
  });

  // --- Initialize all Select2 elements ---
  if (typeof $ !== "undefined") {
    $(".select2-enable").select2({ theme: "default" });
  }

  // --- Flash Message Fade-out ---
  const allFlashMessages = document.querySelectorAll(
    ".flash-error, .flash-success, .flash-warning"
  );
  allFlashMessages.forEach((message, index) => {
    setTimeout(() => {
      message.style.opacity = "0";
    }, 4000 + index * 100);
    setTimeout(() => {
      message.remove();
    }, 4500 + index * 100);
  });

  // --- Collapsible Form Logic ---
  document.querySelectorAll(".collapsible-toggle").forEach((toggleButton) => {
    const content = toggleButton.nextElementSibling;
    if (content && content.classList.contains("form-content")) {
      // Check if it should start open
      if (toggleButton.classList.contains("active")) {
        content.style.maxHeight = content.scrollHeight + "px";
      }

      toggleButton.addEventListener("click", () => {
        toggleButton.classList.toggle("active");
        content.classList.toggle("active");
        if (content.style.maxHeight) {
          content.style.maxHeight = null; // Collapse
        } else {
          content.style.maxHeight = content.scrollHeight + "px"; // Expand
        }
      });
    }
  });

  // --- Ensure tables can scroll horizontally on small screens ---
  document.querySelectorAll("table").forEach((table) => {
    if (!table.parentElement.classList.contains("table-wrapper")) {
      const wrapper = document.createElement("div");
      wrapper.className = "table-wrapper";
      table.parentElement.insertBefore(wrapper, table);
      wrapper.appendChild(table);
    }
  });
});

// Make Select2 init globally available for dynamic rows
function initializeSelect2(selector) {
  if (typeof $ !== "undefined") {
    $(selector).select2({ theme: "default" });
  }
}
