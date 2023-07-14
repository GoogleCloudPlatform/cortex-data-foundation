# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""CLI Prompt routines"""

import json
from typing import Optional, Union

from prompt_toolkit import PromptSession, print_formatted_text
from prompt_toolkit.document import Document
from prompt_toolkit.completion import Completer
from prompt_toolkit.formatted_text import AnyFormattedText, HTML
from prompt_toolkit.formatted_text.pygments import PygmentsTokens
from prompt_toolkit.validation import Validator, ValidationError

from prompt_toolkit.application import Application
from prompt_toolkit.filters import has_focus
from prompt_toolkit.output.color_depth import ColorDepth
from prompt_toolkit.application.current import get_app
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.key_binding.bindings.focus import focus_next, focus_previous
from prompt_toolkit.key_binding.defaults import (load_key_bindings)
from prompt_toolkit.layout import Layout
from prompt_toolkit.layout.containers import VSplit
from prompt_toolkit.widgets import Button, Dialog, Label
from prompt_toolkit.styles import Style

import pygments
from pygments.lexers.shell import BashLexer
from pygments.lexers.web import JsonLexer


class _ValueValidator(Validator):
    """Validates input"""

    def __init__(
        self,
        allow_empty: bool = False,
        completer: Optional[Completer] = None,
        allow_arbitrary: bool = False,
    ):
        self.completer = completer
        self.allow_empty = allow_empty
        self.allow_arbitrary = allow_arbitrary

    def validate(self, document):
        text = document.text

        if not self.allow_empty and text == "":
            raise ValidationError(message="Value cannot be empty.")
        if not self.allow_arbitrary and self.completer is not None:
            if hasattr(self.completer, "words"):
                ignore_case = self.completer.ignore_case # type: ignore
                if self.completer.words: # type: ignore
                    words = self.completer.words # type: ignore
                else:
                    words = []
                text_to_find = (text.lower() if ignore_case
                                else text)
                words = ([w.lower() for w in words]
                            if ignore_case
                            else words)

                invalid = (len(words) > 0 and # type: ignore
                           text_to_find not in words) # type: ignore
            else:
                completions = self.completer.get_completions(
                    Document(text,
                             cursor_position=len(text)), None) # type: ignore
                completions = [p.text for p in completions]
                invalid = text not in completions
            if invalid:
                raise ValidationError(
                    message=f"Value '{text}' is invalid. "
                    "If it refers to a resource, "
                    "make sure it exists and is accessible.",
                    cursor_position=len(text),
                )


def get_value(
    session: PromptSession,
    value_name: str,
    completer: Optional[Completer] = None,
    default_value: str = "",
    allow_empty=False,
    description: Optional[str] = None,
    allow_arbitrary: bool = False,
    min_len: int = 40,
) -> str:
    """Asks user to enter a configuration value

    Args:
        session (PromptSession): PromptSession to use
        value_name (str): Name of the configuration value
        completer (Completer, optional): prompt_toolkit completer to use.
            Defaults to None.
        default_value (str, optional): Default value. Defaults to "".
        allow_empty (bool, optional): Allow empty value. Defaults to False.
        description (str, optional): Description or tip to display
            on the bottom of the screen. Defaults to None.
        allow_arbitrary (bool, optional): Allow any arbitrary value,
            not only what's offered by the completer. Defaults to False.
        min_len (int, optional): Minimal prompt width to fill with _.
            Defaults to 32.

    Returns:
        str: Entered value
    """

    add_spaces_num = min_len - len(value_name) - 1
    if add_spaces_num < 0:
        add_spaces_num = 0
    title = (f"<b><tt bg='ansiwhite' fg='ansiblack'> {value_name}" +
             " " * add_spaces_num + " </tt></b> ")
    session.bottom_toolbar = None
    session.completer = None

    return session.prompt(
        HTML(title),
        completer=completer,
        complete_in_thread=True if completer is not None else False,
        complete_while_typing=True if completer is not None else False,
        validate_while_typing=False,
        default=default_value,
        validator=_ValueValidator(allow_empty, completer, allow_arbitrary),
        bottom_toolbar=HTML(f"<pp fg='ansigreen'>  </pp><b> {description}</b>")
        if description is not None and description != "" else None,
    )


def yes_no(
    title: str,
    message: AnyFormattedText,
    yes_text: str = "YES",
    no_text: str = "NO",
    show_cancel: bool = False,
    full_screen: bool = False,
) -> Optional[bool]:
    """Displays "yes/no" dialog

    Args:
        title (str): Dialog title
        message (str): message/question to display
        yes_text (str, optional): yes-button text. Defaults to "YES".
        no_text (str, optional): no-button text. Defaults to "NO".
        show_cancel (bool, optional): show Cancel button. Defaults to False.
        full_screen (bool, optional): show as a full screen dialog.
                                      Defaults to False.
    Returns:
        bool: True if user pressed yes-button,
              False if user pressed no-button,
              None if user pressed CANCEL.
    """

    def yes_handler() -> None:
        get_app().exit(result=True)

    def no_handler() -> None:
        get_app().exit(result=False)

    def cancel_handler() -> None:
        get_app().exit(result=None)

    buttons = [
        Button(text=yes_text, handler=yes_handler),
        Button(text=no_text, handler=no_handler),
    ]
    if show_cancel:
        buttons.append(Button(text="CANCEL", handler=cancel_handler))

    body = Label(text=message, dont_extend_height=(not full_screen))

    dialog = Dialog(
        title=HTML(title),
        body=body,
        buttons=buttons,
        width=80 if not full_screen else None,
        with_background=True,
    )

    return Application(
        layout=Layout(dialog),
        key_bindings=load_key_bindings(),
        style=Style([
            ("dialog", "bg:default"),
            ("dialog frame.label", "bg:#000000 #FFFFFF"),
        ]),
        mouse_support=True,
        color_depth=ColorDepth.TRUE_COLOR,
        full_screen=full_screen,
        erase_when_done=True,
        enable_page_navigation_bindings=True,
    ).run()


def yes_no_prompt(
    yes_text: str = "YES",
    no_text: str = "NO"
) -> bool:
    def yes_handler() -> None:
        get_app().exit(result=True)

    def no_handler() -> None:
        get_app().exit(result=False)

    buttons=[Button(text=yes_text, handler=yes_handler),
            Button(text=no_text, handler=no_handler)]

    bindings = KeyBindings()
    first_selected = has_focus(buttons[0])
    last_selected = has_focus(buttons[-1])
    # pylint: disable=invalid-unary-operand-type
    bindings.add("left", filter=~first_selected)(focus_previous)
    bindings.add("right", filter=~last_selected)(focus_next)
    bindings.add("s-tab", filter=~first_selected)(focus_previous)
    bindings.add("tab", filter=~last_selected)(focus_next)

    @bindings.add("c-c")
    def _(event):
        """
        Pressing Ctrl-C will exit the user interface with the cancel_value.
        """
        event.app.exit(result=False)

    session = PromptSession(mouse_support=True)
    session.app.layout = Layout(VSplit(buttons, padding=2,
                                       key_bindings=bindings))
    session.app.key_bindings = bindings
    return session.app.run()


def print_formatted(
    text: str,
    bold: bool = False,
    italic: bool = False,
    end: str = "\n",
):
    """Prints formatted text

    Args:
        text (str): Text to print
        bold (bool, optional): Force bold text. Defaults to False.
        italic (bool, optional): Force italic text. Defaults to False.
        end (str, optional): Text to finish output with. Defaults to "\n".
    """
    start_tag = ""
    end_tag = ""
    if italic:
        start_tag = "<i>" + start_tag
        end_tag = "</i>"
    if bold:
        start_tag = "<b>" + start_tag
        end_tag += "</b>"

    text = f"{start_tag}{text}{end_tag}"

    print_formatted_text(
        HTML(text),
        color_depth=ColorDepth.TRUE_COLOR,
        end=end
    )


def print_formatted_bash(script: str):
    """Prints formatted bash script

    Args:
        script (str): bash script to print
    """
    style = Style.from_dict({
        "pygments.name.variable": "bg:#000000 fg:#32AFFF",
        "pygments.literal.string.double": "bg:#000000 fg:ansibrightred",
        "pygments.literal.string.single": "bg:#000000 fg:ansibrightred",
    })
    tokens = list(pygments.lex(script, lexer=BashLexer()))
    print_formatted_text(PygmentsTokens(tokens),
                         color_depth=ColorDepth.TRUE_COLOR,
                         style=style)

def print_formatted_json(json_str_or_dict: Union[str, dict]):
    """Prints formatted JSON

    Args:
        script (str|dict): json string or dictionary
    """

    if isinstance(json_str_or_dict, dict):
        json_str_or_dict = json.dumps(json_str_or_dict, indent=2)

    style = Style.from_dict({
        "pygments.name.variable": "bg:#000000 fg:#32AFFF",
        "pygments.literal.string.double": "bg:#000000 fg:ansibrightred",
        "pygments.literal.string.single": "bg:#000000 fg:ansibrightred",
    })
    tokens = list(pygments.lex(json_str_or_dict, lexer=JsonLexer()))
    print_formatted_text(PygmentsTokens(tokens),
                         color_depth=ColorDepth.TRUE_COLOR,
                         style=style)
