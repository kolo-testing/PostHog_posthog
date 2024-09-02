import {
    IconPencil,
    IconPlus,
    IconSearch,
    IconTrash
} from '@posthog/icons'
import {
    LemonDivider
} from '@posthog/lemon-ui'
import { useActions, useValues } from 'kea'
import { Field, Form, Group } from 'kea-forms'
import { LemonButton } from 'lib/lemon-ui/LemonButton'
import { LemonInput } from 'lib/lemon-ui/LemonInput'

import { SelectorEditingModal } from '~/toolbar/actions/SelectorEditingModal'
import { ToolbarMenu } from '~/toolbar/bar/ToolbarMenu'
import {experimentsTabLogic} from "~/toolbar/experiments/experimentsTabLogic";
import {WebExperimentTransformField} from "~/toolbar/experiments/WebExperimentTransformField";
import { toolbarPosthogJS } from '~/toolbar/toolbarPosthogJS'
import {WebExperimentTransform} from "~/toolbar/types";
import {LemonSlider} from "lib/lemon-ui/LemonSlider";
import React from "react";
import {EditableField} from "lib/components/EditableField/EditableField";



export const ExperimentsEditingToolbarMenu = (): JSX.Element => {
    const {
        selectedExperimentId,
        inspectingElement,
        editingSelector,
        elementsChainBeingEdited,
        editingSelectorValue,
        experimentForm,
    } = useValues(experimentsTabLogic)
    const {
        setExperimentFormValue,
        selectExperiment,
        selectVariant,
        inspectForElementWithIndex,
        deleteExperiment,
        setElementSelector,
        editSelectorWithIndex,
    } = useActions(experimentsTabLogic)

    console.log(`experimentForm is `, experimentForm)
    return (
        <ToolbarMenu>
            <SelectorEditingModal
                isOpen={editingSelector !== null}
                setIsOpen={() => editSelectorWithIndex('', null)}
                activeElementChain={elementsChainBeingEdited}
                startingSelector={editingSelectorValue}
                onChange={(selector) => {
                    if (selector && editingSelector !== null) {
                        toolbarPosthogJS.capture('toolbar_manual_selector_applied', {
                            chosenSelector: selector,
                        })
                        setElementSelector(selector, editingSelector)
                    }
                }}
            />
            <Form
                name="experiment"
                logic={experimentsTabLogic}
                formKey="experimentForm"
                enableFormOnSubmit
                className="flex flex-col overflow-hidden flex-1"
            >
                <ToolbarMenu.Header className="border-b">
                    <h1 className="p-1 font-bold text-sm mb-0">
                        {selectedExperimentId === 'new' ? 'New ' : 'Edit '}
                        experiment
                    </h1>
                </ToolbarMenu.Header>
                <ToolbarMenu.Body>
                    <div className="p-1">
                        <div>
                            <p>Name your experiment</p>
                            <Field name="name">
                                <LemonInput
                                    placeholder="E.g: Hero banner redesign"
                                    className="action-title-field"
                                    stopPropagation={true}
                                    value={experimentForm.name}
                                />
                            </Field>
                        </div>
                        <Group name='variants'>
                            <LemonDivider/>
                            <h4> Variants </h4>
                            <div className="mt-2">
                                <LemonButton
                                    type="secondary"
                                    size="small"
                                    sideIcon={<IconPlus/>}
                                    onClick={() => {
                                        if (experimentForm.variants) {
                                            const nextVariantName = `variant #${Object.keys(experimentForm.variants).length}`
                                            experimentForm.variants[nextVariantName] = {
                                                transforms: [],
                                                conditions: null,
                                            }
                                        }
                                        setExperimentFormValue('variants', experimentForm.variants)
                                    }}
                                >
                                    Add Another Variant ({selectedExperimentId})
                                </LemonButton>
                                {Object.keys(experimentForm.variants!).map((variant, index) => (
                                    <Group key={variant} name={['variants', index]}>
                                        <div className="p-1 flex flex-col gap-2">

                                                {selectedExperimentId === 'new' && (
                                                    <EditableField
                                                        onSave={(newName) => {
                                                            variant = newName
                                                        }}
                                                        name="item-name-small" value={variant} />
                                                )}

                                                {selectedExperimentId !== 'new' && (
                                                    <h3 className='mb-0'>{variant}</h3>
                                                )}
                                                ( rollout percentage :  { experimentForm.variants && experimentForm.variants[variant] ? experimentForm.variants[variant].rollout_percentage : 0} )
                                            <LemonSlider
                                                className="flex-1"
                                                min={0}
                                                max={100}
                                                step={1}
                                                onChange={(value) => {
                                                    if (experimentForm.variants) {
                                                        const webVariant = experimentForm.variants[variant]
                                                        if(webVariant) {
                                                            webVariant.rollout_percentage = value
                                                            setExperimentFormValue('variants', experimentForm.variants)
                                                        }
                                                    }

                                                }}
                                                value={ experimentForm.variants && experimentForm.variants[variant] ? experimentForm.variants[variant].rollout_percentage : 0}
                                            />
                                             <LemonButton
                                                type="secondary"
                                                size="small"
                                                sideIcon={<IconPlus/>}
                                                onClick={() => {
                                                    if (experimentForm.variants) {
                                                        const webVariant = experimentForm.variants[variant]
                                                        if (webVariant) {
                                                            if (webVariant.transforms) {
                                                                webVariant.transforms.push({
                                                                 text: "",
                                                                 html: "",
                                                                } as unknown as WebExperimentTransform)
                                                            }
                                                        }
                                                        setExperimentFormValue('variants', experimentForm.variants)
                                                }}}
                                            >
                                                Add Another Element
                                            </LemonButton>
                                            <LemonDivider/>

                                            {experimentForm.variants![variant].transforms.map((transform, tIndex) => (
                                                <div key={tIndex}> {transform.selector}
                                                        <div className="action-inspect">
                                                            <LemonButton
                                                                size="small"
                                                                type={inspectingElement === tIndex ? 'primary' : 'secondary'}
                                                                onClick={(e) => {
                                                                    e.stopPropagation()
                                                                    selectVariant(variant)
                                                                    console.log(`calling inspectForElementWithIndex(${variant}, ${tIndex +1})`)
                                                                    inspectForElementWithIndex(variant, tIndex +1)
                                                                }}
                                                                icon={<IconSearch />}
                                                            >
                                                                {transform.selector ? 'Change Element' : 'Select Element'}
                                                            </LemonButton>
                                                            <WebExperimentTransformField tIndex={tIndex} variant={variant} transform={transform} />

                                                            {inspectingElement === tIndex ? (
                                                                <>
                                                                    <div className="flex flex-row justify-end mb-2">
                                                                        <LemonButton
                                                                            size="small"
                                                                            type="secondary"
                                                                            icon={<IconPencil />}
                                                                            onClick={(e) => {
                                                                                e.stopPropagation()
                                                                                toolbarPosthogJS.capture(
                                                                                    'toolbar_manual_selector_modal_opened',
                                                                                    {
                                                                                        selector: transform.selector,
                                                                                    }
                                                                                )
                                                                                editSelectorWithIndex(variant, tIndex)
                                                                            }}
                                                                        >
                                                                            Edit the selector
                                                                        </LemonButton>
                                                                    </div>
                                                                </>
                                                            ): null}
                                                        </div>
                                                   </div>
                                            ))}
                                            </div>
                                    </Group>
                                ))}
                            </div>
                        </Group>
                    </div>
                </ToolbarMenu.Body>
                <ToolbarMenu.Footer>
                    <span className="flex-1">
                        {selectedExperimentId !== 'new' ? (
                            <LemonButton
                                type="secondary"
                                status="danger"
                                onClick={deleteExperiment}
                                icon={<IconTrash />}
                                size="small"
                            >
                                Delete
                            </LemonButton>
                        ) : null}
                    </span>
                    <LemonButton type="secondary" size="small" onClick={() => selectExperiment(null)}>
                        Cancel
                    </LemonButton>
                    <LemonButton type="primary" htmlType="submit" size="small">
                        {selectedExperimentId === 'new' ? 'Create ' : 'Save '}
                        experiment
                    </LemonButton>
                </ToolbarMenu.Footer>
            </Form>
        </ToolbarMenu>
    )
}
