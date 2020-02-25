from colorama import Fore, Style

def convertShellToHTML(result_message: str) -> str: 
    result_message = result_message.replace(Fore.GREEN, '<font color="green">')
    result_message = result_message.replace(Fore.BLUE, '<font color="blue">')
    result_message = result_message.replace(Fore.RED, '<font color="red">')
    result_message = result_message.replace(Style.RESET_ALL, '</font>')
    return result_message