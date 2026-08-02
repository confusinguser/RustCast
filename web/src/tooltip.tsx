// A single app-wide react-tooltip anchor. Elements opt in by spreading
// `tip("text")` onto themselves (sets the data-attributes react-tooltip reads),
// which reads nicer than native `title=` tooltips.
import { Tooltip } from "react-tooltip";
import "react-tooltip/dist/react-tooltip.css";

export const TOOLTIP_ID = "rc-tip";

/** Props that anchor an element to the shared tooltip. Empty when no content. */
export function tip(content?: string) {
  return content ? { "data-tooltip-id": TOOLTIP_ID, "data-tooltip-content": content } : {};
}

/** Rendered once near the app root. */
export function TooltipHost() {
  return (
    <Tooltip
      id={TOOLTIP_ID}
      opacity={1}
      noArrow
      style={{
        zIndex: 60,
      }}
    />
  );
}
