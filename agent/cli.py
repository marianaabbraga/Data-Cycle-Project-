"""
CLI chat interface for the Pipeline Assistant.

Run:
    python -m agent.cli
"""

from agent.chat_agent import agent


def main():
    print("Pipeline Assistant (type 'exit' to quit)")
    print("-" * 45)

    while True:
        try:
            user_input = input("\nyou> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye.")
            break

        if not user_input:
            continue
        if user_input.lower() in ("exit", "quit", "q"):
            print("Bye.")
            break

        response = agent.say(user_input)
        print(f"\nassistant> {response}")


if __name__ == "__main__":
    main()
