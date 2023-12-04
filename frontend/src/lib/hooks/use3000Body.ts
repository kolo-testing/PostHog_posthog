import { useValues } from 'kea'
import { useEffect } from 'react'

import { themeLogic } from '~/layout/navigation-3000/themeLogic'

export function use3000Body(): void {
    const { isDarkModeOn, is3000 } = useValues(themeLogic)

    useEffect(() => {
        if (is3000) {
            document.body.setAttribute('theme', isDarkModeOn ? 'dark' : 'light')
            document.body.classList.add('posthog-3000')
        } else {
            document.body.classList.remove('posthog-3000')
        }
    }, [is3000, isDarkModeOn])
}
