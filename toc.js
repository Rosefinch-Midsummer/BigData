// Populate the sidebar
//
// This is a script, and not included directly in the page, to control the total size of the book.
// The TOC contains an entry for each page, so if each page includes a copy of the TOC,
// the total size of the page becomes O(n**2).
class MDBookSidebarScrollbox extends HTMLElement {
    constructor() {
        super();
    }
    connectedCallback() {
        this.innerHTML = '<ol class="chapter"><li class="chapter-item "><a href="前言.html"><strong aria-hidden="true">1.</strong> 前言</a></li><li class="chapter-item "><a href="Hadoop/Hadoop.html"><strong aria-hidden="true">2.</strong> Hadoop</a><a class="toggle"><div>❱</div></a></li><li><ol class="section"><li class="chapter-item "><a href="Hadoop/HDFS.html"><strong aria-hidden="true">2.1.</strong> HDFS</a><a class="toggle"><div>❱</div></a></li><li><ol class="section"><li class="chapter-item "><a href="Hadoop/HDFS概述.html"><strong aria-hidden="true">2.1.1.</strong> HDFS概述</a></li><li class="chapter-item "><a href="Hadoop/HDFS读写流程.html"><strong aria-hidden="true">2.1.2.</strong> HDFS读写流程</a></li><li class="chapter-item "><a href="Hadoop/HDFS-Shell操作.html"><strong aria-hidden="true">2.1.3.</strong> HDFS-Shell操作</a></li><li class="chapter-item "><a href="Hadoop/HDFS-API操作.html"><strong aria-hidden="true">2.1.4.</strong> HDFS-API操作</a></li></ol></li><li class="chapter-item "><a href="Hadoop/MapReduce.html"><strong aria-hidden="true">2.2.</strong> MapReduce</a><a class="toggle"><div>❱</div></a></li><li><ol class="section"><li class="chapter-item "><a href="Hadoop/MapReduce概述.html"><strong aria-hidden="true">2.2.1.</strong> MapReduce概述</a></li><li class="chapter-item "><a href="Hadoop/序列化.html"><strong aria-hidden="true">2.2.2.</strong> 序列化</a></li><li class="chapter-item "><a href="Hadoop/MapReduce框架原理.html"><strong aria-hidden="true">2.2.3.</strong> MapReduce框架原理</a></li></ol></li><li class="chapter-item "><a href="Hadoop/Hadoop数据压缩.html"><strong aria-hidden="true">2.3.</strong> Hadoop数据压缩</a></li></ol></li><li class="chapter-item "><a href="Spark/Spark概述.html"><strong aria-hidden="true">3.</strong> Spark</a><a class="toggle"><div>❱</div></a></li><li><ol class="section"><li class="chapter-item "><a href="Spark/Spark核心概念.html"><strong aria-hidden="true">3.1.</strong> Spark核心概念</a></li><li class="chapter-item "><a href="Spark/SparkRDD.html"><strong aria-hidden="true">3.2.</strong> SparkRDD</a></li><li class="chapter-item "><a href="Spark/SparkCore.html"><strong aria-hidden="true">3.3.</strong> SparkCore</a></li><li class="chapter-item "><a href="Spark/SparkCore实战.html"><strong aria-hidden="true">3.4.</strong> SparkCore实战</a></li><li class="chapter-item "><a href="Spark/SparkSQL.html"><strong aria-hidden="true">3.5.</strong> SparkSQL</a></li><li class="chapter-item "><a href="Spark/SparkSQL实战.html"><strong aria-hidden="true">3.6.</strong> SparkSQL实战</a></li><li class="chapter-item "><a href="Spark/Spark数据源.html"><strong aria-hidden="true">3.7.</strong> Spark数据源</a></li><li class="chapter-item "><a href="Spark/SparkStreaming.html"><strong aria-hidden="true">3.8.</strong> SparkStreaming</a></li></ol></li></ol>';
        // Set the current, active page, and reveal it if it's hidden
        let current_page = document.location.href.toString().split("#")[0];
        if (current_page.endsWith("/")) {
            current_page += "index.html";
        }
        var links = Array.prototype.slice.call(this.querySelectorAll("a"));
        var l = links.length;
        for (var i = 0; i < l; ++i) {
            var link = links[i];
            var href = link.getAttribute("href");
            if (href && !href.startsWith("#") && !/^(?:[a-z+]+:)?\/\//.test(href)) {
                link.href = path_to_root + href;
            }
            // The "index" page is supposed to alias the first chapter in the book.
            if (link.href === current_page || (i === 0 && path_to_root === "" && current_page.endsWith("/index.html"))) {
                link.classList.add("active");
                var parent = link.parentElement;
                if (parent && parent.classList.contains("chapter-item")) {
                    parent.classList.add("expanded");
                }
                while (parent) {
                    if (parent.tagName === "LI" && parent.previousElementSibling) {
                        if (parent.previousElementSibling.classList.contains("chapter-item")) {
                            parent.previousElementSibling.classList.add("expanded");
                        }
                    }
                    parent = parent.parentElement;
                }
            }
        }
        // Track and set sidebar scroll position
        this.addEventListener('click', function(e) {
            if (e.target.tagName === 'A') {
                sessionStorage.setItem('sidebar-scroll', this.scrollTop);
            }
        }, { passive: true });
        var sidebarScrollTop = sessionStorage.getItem('sidebar-scroll');
        sessionStorage.removeItem('sidebar-scroll');
        if (sidebarScrollTop) {
            // preserve sidebar scroll position when navigating via links within sidebar
            this.scrollTop = sidebarScrollTop;
        } else {
            // scroll sidebar to current active section when navigating via "next/previous chapter" buttons
            var activeSection = document.querySelector('#sidebar .active');
            if (activeSection) {
                activeSection.scrollIntoView({ block: 'center' });
            }
        }
        // Toggle buttons
        var sidebarAnchorToggles = document.querySelectorAll('#sidebar a.toggle');
        function toggleSection(ev) {
            ev.currentTarget.parentElement.classList.toggle('expanded');
        }
        Array.from(sidebarAnchorToggles).forEach(function (el) {
            el.addEventListener('click', toggleSection);
        });
    }
}
window.customElements.define("mdbook-sidebar-scrollbox", MDBookSidebarScrollbox);
