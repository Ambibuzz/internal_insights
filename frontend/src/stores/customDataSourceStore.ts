import * as api from '@/api'
import dayjs from '@/utils/dayjs'
import { defineStore } from 'pinia'
import { computed } from 'vue'

const useCustomDataSourceStore = defineStore('insights:custom_data_sources', () => {
	const listResource = api.getListResource({
		doctype: 'Insights Custom Data Source',
		filters: {},
		fields: [
			'title',
			'status',
			'creation',
			'modified',
			'database_type',
		],
		orderBy: 'creation desc',
		pageLength: 100,
		auto: true,
	})

	const list = computed<CustomDataSourceItem[]>(
		() =>
			listResource.list.data?.map((dataSource: CustomDataSourceItem) => {
				dataSource.created_from_now = dayjs(dataSource.creation).fromNow()
				dataSource.modified_from_now = dayjs(dataSource.modified).fromNow()
				dataSource.title = window.location.hostname || dataSource.title
				return dataSource
			}) || []
	)
	const dropdownOptions = computed<DropdownOption[]>(() => list.value.map(makeDropdownOption))

	function makeDropdownOption(dataSource: CustomDataSourceItem): DropdownOption {
		return {
			label: dataSource.title,
			value: dataSource.database_type,
			description: dataSource.database_type,
		}
	}

	return {
		list,
		dropdownOptions,
		loading: listResource.list.loading,
		testing: api.testCustomDataSourceConnection.loading,
		creating: api.createCustomDataSource.loading,
		deleting: listResource.delete.loading,
		create: (args: any) => api.createCustomDataSource.submit(args).then(() => listResource.list.reload()),
		delete: (name: string) =>
			listResource.delete.submit(name).then(() => listResource.list.reload()),
		testConnection: (args: any) => api.testCustomDataSourceConnection.submit(args),
		getDropdownOptions: (filters: any) => {
			// filters = {is_site_db: 1}
			const filteredDataSources = list.value.filter((dataSource: CustomDataSourceItem) => {
				for (const key in filters) {
					const k = key as keyof CustomDataSourceItem
					if (dataSource[k] != filters[k]) {
						return false
					}
				}
				return true
			})
			return filteredDataSources.map(makeDropdownOption)
		},
	}
})

export default useCustomDataSourceStore
export type DataSourceStore = ReturnType<typeof useCustomDataSourceStore>
