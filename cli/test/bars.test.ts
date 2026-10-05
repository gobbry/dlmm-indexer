import { expect, test } from "bun:test";
import { renderBar, renderBars } from "../src/bars.ts";

test.each([
  { name: "largest bucket fills the width", value: 10, max: 10, width: 4, expected: "████" },
  { name: "half fills half", value: 5, max: 10, width: 4, expected: "██  " },
  { name: "remainder uses an eighth block", value: 3, max: 8, width: 1, expected: "▍" },
  { name: "tiny non-zero value still shows", value: 0.001, max: 1_000, width: 4, expected: "▏   " },
  { name: "zero is blank", value: 0, max: 10, width: 3, expected: "   " },
  { name: "all-zero maximum is blank", value: 0, max: 0, width: 2, expected: "  " },
])("renderBar: $name", ({ value, max, width, expected }) => {
  expect(renderBar(value, max, width)).toBe(expected);
});

test("renderBars scales every bucket to the largest and keeps a fixed width", () => {
  const bars = renderBars([1, 4, 2], 8);
  expect(bars).toEqual(["██      ", "████████", "████    "]);
});
