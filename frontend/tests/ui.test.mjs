import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { Toaster } from "../js/ui.js";

/**
 * Just enough of a document for the toaster.
 *
 * The alternative — a full DOM implementation as a dependency — would be a large
 * amount of machinery to observe one list of children.
 */
function fakeDocument() {
  const make = (tag) => ({
    tagName: tag,
    className: "",
    type: "",
    textContent: "",
    dataset: {},
    children: [],
    attributes: {},
    listeners: {},
    setAttribute(name, value) {
      this.attributes[name] = value;
    },
    addEventListener(name, handler) {
      this.listeners[name] = handler;
    },
    append(...nodes) {
      for (const node of nodes) {
        node.parent = this;
        this.children.push(node);
      }
    },
    remove() {
      const siblings = this.parent?.children;
      if (siblings) siblings.splice(siblings.indexOf(this), 1);
    },
    querySelector(selector) {
      const match = /^\[data-toast-id="(\d+)"\]$/.exec(selector);
      if (!match) return null;
      return this.children.find((child) => child.dataset.toastId === match[1]) ?? null;
    },
  });
  return { createElement: make, container: make("div") };
}

/** A toaster wired to the fake document, with timers that never fire on their own. */
function build({ maxVisible } = {}) {
  const { createElement, container } = fakeDocument();
  const timers = {
    setTimeout: () => 0,
    clearTimeout: () => {},
  };
  return {
    container,
    toaster: new Toaster(container, { doc: { createElement }, timers, maxVisible }),
    ids: () => container.children.map((child) => Number(child.dataset.toastId)),
  };
}

describe("Toaster", () => {
  it("shows what it is given", () => {
    const { toaster, container } = build();
    toaster.show({ title: "Device entered a zone", detail: "enter - dev-1" });

    assert.equal(container.children.length, 1);
    assert.equal(container.children[0].children[0].children[0].textContent, "Device entered a zone");
  });

  it("dismisses by id", () => {
    const { toaster, container } = build();
    const id = toaster.show({ title: "one" });
    toaster.dismiss(id);

    assert.equal(container.children.length, 0);
  });

  it("never stacks more than the ceiling, however fast alerts arrive", () => {
    const { toaster, ids } = build({ maxVisible: 4 });
    for (let index = 0; index < 40; index += 1) toaster.show({ title: `alert ${index}` });

    assert.equal(ids().length, 4);
  });

  it("drops the oldest first, so the newest alert is always the one on screen", () => {
    const { toaster, ids } = build({ maxVisible: 3 });
    const shown = [1, 2, 3, 4, 5].map((n) => toaster.show({ title: `alert ${n}` }));

    assert.deepEqual(ids(), shown.slice(-3));
  });

  it("keeps a message that asked to stay, and drops a transient one instead", () => {
    // A slow-consumer disconnect or a session limit is shown with no timeout because the
    // user has to act on it; a burst of alerts must not push it off the screen.
    const { toaster, ids } = build({ maxVisible: 2 });
    const sticky = toaster.show({ title: "Too many sessions", timeoutMs: 0 });
    toaster.show({ title: "alert 1" });
    const newest = toaster.show({ title: "alert 2" });

    assert.deepEqual(ids(), [sticky, newest]);
  });

  it("gives up and drops the oldest sticky message when only sticky ones remain", () => {
    const { toaster, ids } = build({ maxVisible: 2 });
    const first = toaster.show({ title: "first", timeoutMs: 0 });
    const second = toaster.show({ title: "second", timeoutMs: 0 });
    const third = toaster.show({ title: "third", timeoutMs: 0 });

    assert.equal(ids().includes(first), false);
    assert.deepEqual(ids(), [second, third]);
  });
});
