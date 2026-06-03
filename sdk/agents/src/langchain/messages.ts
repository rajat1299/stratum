interface MessageLike {
  content?: unknown;
}

interface ContentBlock {
  type?: string;
  text?: string;
}

function isTextBlock(value: unknown): value is { type: "text"; text: string } {
  if (typeof value !== "object" || value === null) return false;
  const block = value as ContentBlock;
  return block.type === "text" && typeof block.text === "string";
}

/**
 * Provider-free helper to pull plain-text content out of LangChain-style
 * messages, supporting both plain string content and content block arrays.
 */
export function extractText(messages: readonly MessageLike[]): string[] {
  const texts: string[] = [];
  for (const message of messages) {
    if (!("content" in message) || message.content === undefined || message.content === null) continue;
    const content = message.content;
    if (typeof content === "string") {
      if (content.trim().length > 0) texts.push(content);
      continue;
    }
    if (Array.isArray(content)) {
      for (const block of content) {
        if (isTextBlock(block) && block.text.trim().length > 0) {
          texts.push(block.text);
        }
      }
    }
  }
  return texts;
}
